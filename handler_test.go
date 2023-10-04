package main

import (
	"context"
	"testing"

	daskv1alpha1 "github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1"
	"github.com/CHARM-Tx/dask-operator/pkg/generated/clientset/fake"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2/ktesting"
)

type fixture struct {
	t *testing.T

	kubeclient *k8sfake.Clientset
	client     *fake.Clientset
}

func newFixture(t *testing.T, objects []runtime.Object, kubeObjects []runtime.Object) *fixture {
	return &fixture{
		t: t,

		kubeclient: k8sfake.NewSimpleClientset(kubeObjects...),
		client:     fake.NewSimpleClientset(objects...),
	}
}

func (f *fixture) newController(ctx context.Context, objects []runtime.Object) *Controller {
	controller := NewController(f.kubeclient, f.client, ctx)
	for _, o := range objects {
		switch o := o.(type) {
		case *daskv1alpha1.Cluster:
			controller.clusters.Informer().GetIndexer().Add(o)
		}
	}

	for _, factory := range controller.factories {
		factory.Start(ctx.Done())
	}

	return controller
}

func getKey(cluster *daskv1alpha1.Cluster, t *testing.T) string {
	key, err := cache.MetaNamespaceKeyFunc(cluster)
	if err != nil {
		t.Errorf("error getting key for cluster %v: %v", cluster, err)
		return ""
	}
	return key
}

func filterActions(actions []k8stesting.Action) []k8stesting.Action {
	filteredActions := make([]k8stesting.Action, 0, len(actions))
	for _, action := range actions {
		if action.GetVerb() == "list" || action.GetVerb() == "watch" {
			continue
		}
		filteredActions = append(filteredActions, action)
	}

	return filteredActions
}

func TestCreatesResources(t *testing.T) {
	cluster := daskv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "bar"},
		Spec: daskv1alpha1.ClusterSpec{
			Scheduler: daskv1alpha1.SchedulerSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:    "scheduler",
								Image:   "ubuntu:22.04",
								Command: []string{"dask", "scheduler"},
							},
						},
					},
				},
				Service: corev1.ServiceSpec{},
			},
			Worker: daskv1alpha1.WorkerSpec{
				MinReplicas: 0,
				Replicas:    1,
				MaxReplicas: 1,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:    "worker",
								Image:   "ubuntu:22.04",
								Command: []string{"dask", "worker"},
							},
						},
					},
				},
			},
		},
	}
	kubeObjects := []runtime.Object{}
	objects := []runtime.Object{&cluster}
	f := newFixture(t, objects, kubeObjects)

	_, ctx := ktesting.NewTestContext(t)
	controller := f.newController(ctx, objects)

	if err := controller.syncHandler(ctx, getKey(&cluster, t)); err != nil {
		t.Fatalf("Error syncing cluster: %v", err)
	}

	expectedDeployment, err := buildSchedulerDeployment("foo-scheduler", &cluster)
	if err != nil {
		t.Fatalf("unable to construct expected deployment: %s", err)
	}

	expectedPod, err := buildWorkerPod("foo-worker", "foo-scheduler", &cluster)
	if err != nil {
		t.Fatalf("unable to construct expected worker pod: %s", err)
	}
	expectedActions := []k8stesting.Action{
		k8stesting.NewCreateAction(
			schema.GroupVersionResource{Resource: "services"},
			cluster.Namespace,
			buildSchedulerService("foo-scheduler", &cluster),
		),
		k8stesting.NewCreateAction(
			schema.GroupVersionResource{Group: "apps", Resource: "deployments"},
			cluster.Namespace,
			expectedDeployment,
		),
		k8stesting.NewCreateAction(
			schema.GroupVersionResource{Resource: "pods"},
			cluster.Namespace,
			expectedPod,
		),
	}

	actions := filterActions(f.kubeclient.Actions())
	if len(actions) != len(expectedActions) {
		t.Errorf("expected %d actions, got %d", len(expectedActions), len(actions))
	}

	for i, action := range actions {
		expectedAction := expectedActions[i]
		if !(expectedAction.Matches(action.GetVerb(), action.GetResource().Resource) && action.GetSubresource() == expectedAction.GetSubresource()) {
			t.Errorf("action %v does not match expected action %v", action, expectedAction)
		}
	}
}
