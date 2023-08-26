package main

import (
	"context"
	"fmt"
	"time"

	"github.com/CHARM-Tx/dask-operator/pkg/apis/dask"
	"github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1"
	"github.com/CHARM-Tx/dask-operator/pkg/generated/clientset"
	informers "github.com/CHARM-Tx/dask-operator/pkg/generated/informers/externalversions"
	listers "github.com/CHARM-Tx/dask-operator/pkg/generated/listers/dask/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type Controller struct {
	clusterLister    listers.ClusterLister
	deploymentLister appsv1listers.DeploymentLister
	podLister        corev1listers.PodLister

	workqueue workqueue.RateLimitingInterface
}

func NewController(kubeclient kubernetes.Interface, daskclient clientset.Interface, ctx context.Context) *Controller {
	logger := klog.FromContext(ctx)

	daskInformerFactory := informers.NewSharedInformerFactory(daskclient, 0*time.Second)
	kubeInformerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
		kubeclient,
		0*time.Second,
		k8sinformers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = fmt.Sprintf("%s/cluster=", dask.GroupName)
		}),
	)
	pods := kubeInformerFactory.Core().V1().Pods()
	deployments := kubeInformerFactory.Apps().V1().Deployments()
	clusters := daskInformerFactory.Dask().V1alpha1().Clusters()

	controller := &Controller{
		podLister:        pods.Lister(),
		deploymentLister: deployments.Lister(),
		clusterLister:    clusters.Lister(),
		workqueue:        workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
	}

	logger.Info("Setting up event handlers")
	pods.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueObject,
		UpdateFunc: func(old, new interface{}) {
			oldPod := old.(*corev1.Pod)
			newPod := new.(*corev1.Pod)
			if oldPod.ResourceVersion != newPod.ResourceVersion {
				controller.enqueueObject(newPod)
			}
		},
		DeleteFunc: controller.enqueueObject,
	})
	deployments.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueObject,
		UpdateFunc: func(old, new interface{}) {
			oldDeployment := old.(*appsv1.Deployment)
			newDeployment := new.(*appsv1.Deployment)
			if oldDeployment.ResourceVersion != newDeployment.ResourceVersion {
				controller.enqueueObject(newDeployment)
			}
		},
		DeleteFunc: controller.enqueueObject,
	})
	clusters.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueCluster,
		UpdateFunc: func(old, new interface{}) {
			oldCluster := old.(*v1alpha1.Cluster)
			newCluster := new.(*v1alpha1.Cluster)
			if oldCluster.ResourceVersion != newCluster.ResourceVersion {
				controller.enqueueCluster(newCluster)
			}
		},
	})

	kubeInformerFactory.Start(ctx.Done())
	daskInformerFactory.Start(ctx.Done())

	return controller
}

func (c *Controller) enqueueObject(obj interface{}) {
	logger := klog.FromContext(context.Background())

	object, ok := obj.(metav1.Object)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding tombstone, invalid type"))
		}
	}

	ownerRef := metav1.GetControllerOf(object)
	if ownerRef == nil {
		return
	}
	if schema.FromAPIVersionAndKind(ownerRef.APIVersion, ownerRef.Kind) == v1alpha1.SchemeGroupVersion.WithKind("Cluster") {
		return
	}

	cluster, err := c.clusterLister.Clusters(object.GetNamespace()).Get(ownerRef.Name)
	if err != nil {
		logger.Info("Ignoring orphaned object", "object", klog.KObj(object), "cluster", ownerRef.Name)
	}

	c.enqueueCluster(cluster)
}

func (c *Controller) enqueueCluster(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) syncHandler(ctx context.Context, key string) error {
	return nil
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := c.workqueue.Get()
	logger := klog.FromContext(ctx)

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)

		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		if err := c.syncHandler(ctx, key); err != nil {
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeueing", key, err.Error())
		}

		c.workqueue.Forget(obj)
		logger.Info("Successfully synced", "resourceName", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *Controller) Run(workers int, ctx context.Context) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting workers", "count", workers)
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
}
