package main

import (
	"context"
	"fmt"

	daskv1alpha1 "github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1"
	daskv1alpha1ac "github.com/CHARM-Tx/dask-operator/pkg/generated/applyconfigurations/dask/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
)

func (c *Controller) syncHandler(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid key: %s", key))
		return err
	}

	cluster, err := c.clusters.Lister().Clusters(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("cluster no longer exists: %s/%s", namespace, name))
			return nil
		}
		return err
	}

	scheduler, err := c.handleScheduler(ctx, cluster)
	if err != nil {
		return err
	}
	if err := c.handleWorker(ctx, scheduler, cluster); err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleScheduler(ctx context.Context, cluster *daskv1alpha1.Cluster) (*corev1.Service, error) {
	fieldManager := "dask-operator"
	name := fmt.Sprintf("%s-scheduler", cluster.Name)

	service, err := c.services.Lister().Services(cluster.Namespace).Get(name)
	if errors.IsNotFound(err) {
		service = buildSchedulerService(name, cluster)
		service, err = c.kubeclient.CoreV1().Services(cluster.Namespace).Create(ctx, service, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(service, cluster) {
		return nil, fmt.Errorf("service %s already exists, and is not owned by a cluster", name)
	}
	serviceAddress := fmt.Sprintf("%s.%s.svc", name, cluster.Namespace)

	deployment, err := c.deployments.Lister().Deployments(cluster.Namespace).Get(name)
	if errors.IsNotFound(err) {
		deployment, err = buildSchedulerDeployment(name, cluster)
		if err != nil {
			return nil, err
		}
		deployment, err = c.kubeclient.AppsV1().Deployments(cluster.Namespace).Create(ctx, deployment, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(deployment, cluster) {
		return nil, fmt.Errorf("deployment %s already exists, and is not owned by a cluster", name)
	}

	ac := daskv1alpha1ac.Cluster(cluster.Name, cluster.Namespace).WithStatus(
		daskv1alpha1ac.ClusterStatus().WithScheduler(
			daskv1alpha1ac.SchedulerStatus().WithAddress(serviceAddress),
		),
	)
	clusterClient := c.daskclient.DaskV1alpha1().Clusters(cluster.Namespace)
	_, err = clusterClient.Apply(ctx, ac, metav1.ApplyOptions{FieldManager: fieldManager})
	if err != nil {
		return nil, err
	}

	return service, nil
}

func (c *Controller) handleWorker(ctx context.Context, scheduler *corev1.Service, cluster *daskv1alpha1.Cluster) error {
	fieldManager := "dask-operator"
	name := fmt.Sprintf("%s-worker", cluster.Name)

	pods, err := c.pods.Lister().Pods(cluster.Namespace).List(labels.SelectorFromSet(clusterLabels(cluster, "worker")))
	if err != nil {
		return err
	}
	podIds := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		podIds[pod.Name] = struct{}{}
	}

	pod, err := buildWorkerPod(name, scheduler, cluster)
	if err != nil {
		return err
	}

	status := daskv1alpha1ac.WorkerStatus().WithCount(int32(len(pods)))
	for _, retiringPod := range cluster.Status.Workers.Retiring {
		if _, ok := podIds[retiringPod.Id]; ok {
			// This pod hasn't terminated yet, keep tracking it in the retiring list
			status.WithRetiring(daskv1alpha1ac.RetiredWorker().WithId(retiringPod.Id))
		}
	}

	desiredPods := int(cluster.Spec.Worker.Replicas) - len(pods) + len(cluster.Status.Workers.Retiring)
	if desiredPods > 0 {
		for i := 0; i < desiredPods; i++ {
			// TODO: Some sort of rate limiting or sanity check
			c.kubeclient.CoreV1().Pods(cluster.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		}
	} else if desiredPods < 0 {
		retirings, err := c.scheduler.retireWorkers(cluster, -desiredPods)
		if err != nil {
			return err
		}

		for _, retiring := range retirings {
			status.WithRetiring(daskv1alpha1ac.RetiredWorker().WithId(retiring.Id))
		}
	} else {
		return nil
	}

	ac := daskv1alpha1ac.Cluster(cluster.Name, cluster.Namespace).WithStatus(
		daskv1alpha1ac.ClusterStatus().WithWorkers(status),
	)
	clusterClient := c.daskclient.DaskV1alpha1().Clusters(cluster.Namespace)
	_, err = clusterClient.Apply(ctx, ac, metav1.ApplyOptions{FieldManager: fieldManager})
	if err != nil {
		return err
	}

	return nil
}
