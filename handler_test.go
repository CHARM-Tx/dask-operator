package main

import (
	"context"
	"testing"

	daskv1alpha1 "github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1"
	"github.com/CHARM-Tx/dask-operator/pkg/generated/clientset/fake"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2/ktesting"
)

type fakeSchedulerClient struct {
	events []RetireResult
}

func (c *fakeSchedulerClient) retireWorkers(cluster *daskv1alpha1.Cluster, n int) (RetireResult, error) {
	retiredWorkers := make(RetireResult, 0, n)
	for i := 0; i < n; i++ {
		retiredWorkers = append(retiredWorkers, struct{ id string }{id: "foo"})
	}
	c.events = append(c.events, retiredWorkers)
	return retiredWorkers, nil
}

type fixture struct {
	t *testing.T

	kubeclient      *k8sfake.Clientset
	client          *fake.Clientset
	schedulerclient *fakeSchedulerClient
}

func newFixture(t *testing.T, objects []runtime.Object, kubeObjects []runtime.Object) *fixture {
	return &fixture{
		t: t,

		kubeclient:      k8sfake.NewSimpleClientset(kubeObjects...),
		client:          fake.NewSimpleClientset(objects...),
		schedulerclient: &fakeSchedulerClient{events: make([]RetireResult, 0)},
	}
}

func (f *fixture) newController(ctx context.Context, objects, kubeObjects []runtime.Object) *Controller {
	controller := NewController(f.kubeclient, f.client, f.schedulerclient, ctx)
	for _, o := range objects {
		switch o := o.(type) {
		case *daskv1alpha1.Cluster:
			controller.clusters.Informer().GetIndexer().Add(o)
		}
	}

	for _, o := range kubeObjects {
		switch o := o.(type) {
		case *corev1.Service:
			controller.services.Informer().GetIndexer().Add(o)
		case *corev1.Pod:
			controller.pods.Informer().GetIndexer().Add(o)
		case *appsv1.Deployment:
			controller.deployments.Informer().GetIndexer().Add(o)
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

func makeCluster() daskv1alpha1.Cluster {
	return daskv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "bar"},
		Spec: daskv1alpha1.ClusterSpec{
			Scheduler: daskv1alpha1.SchedulerSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:    "scheduler",
								Image:   "ghcr.io/dask/dask:latest",
								Command: []string{"dask", "scheduler"},
							},
						},
					},
				},
				Service: corev1.ServiceSpec{},
			},
			Worker: daskv1alpha1.WorkerSpec{
				Replicas: 0,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:    "worker",
								Image:   "ghcr.io/dask/dask:latest",
								Command: []string{"dask", "worker"},
							},
						},
					},
				},
			},
		},
	}
}

func TestCreatesResources(t *testing.T) {
	cluster := makeCluster()
	cluster.Spec.Worker.Replicas = 1
	kubeObjects := []runtime.Object{}
	objects := []runtime.Object{&cluster}
	f := newFixture(t, objects, kubeObjects)

	_, ctx := ktesting.NewTestContext(t)
	controller := f.newController(ctx, objects, kubeObjects)

	if err := controller.syncHandler(ctx, getKey(&cluster, t)); err != nil {
		t.Fatalf("Error syncing cluster: %v", err)
	}

	expectedScheduler := buildSchedulerService("foo-scheduler", &cluster)
	expectedDeployment, err := buildSchedulerDeployment("foo-scheduler", &cluster)
	if err != nil {
		t.Fatalf("unable to construct expected deployment: %s", err)
	}

	expectedPod, err := buildWorkerPod("foo-worker", expectedScheduler, &cluster)
	if err != nil {
		t.Fatalf("unable to construct expected worker pod: %s", err)
	}
	expectedActions := []k8stesting.Action{
		k8stesting.NewCreateAction(
			schema.GroupVersionResource{Resource: "services"},
			cluster.Namespace,
			expectedScheduler,
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

func TestIdle(t *testing.T) {
	cluster := makeCluster()
	cluster.Spec.Worker.Replicas = 1

	ownerRefs := []metav1.OwnerReference{*metav1.NewControllerRef(&cluster, daskv1alpha1.SchemeGroupVersion.WithKind("Cluster"))}
	kubeObjects := []runtime.Object{
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "foo-scheduler", Namespace: "bar", OwnerReferences: ownerRefs},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "tcp-comm", Port: 8786}}},
		},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "foo-scheduler", Namespace: "bar", OwnerReferences: ownerRefs}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       "bar",
			Labels:          clusterLabels(&cluster, "worker"),
			OwnerReferences: ownerRefs,
		}},
	}
	objects := []runtime.Object{&cluster}
	f := newFixture(t, objects, kubeObjects)

	_, ctx := ktesting.NewTestContext(t)
	controller := f.newController(ctx, objects, kubeObjects)

	if err := controller.syncHandler(ctx, getKey(&cluster, t)); err != nil {
		t.Fatalf("Error syncing cluster: %v", err)
	}

	expectedActions := []k8stesting.Action{}
	actions := filterActions(f.kubeclient.Actions())
	if len(actions) != len(expectedActions) {
		t.Errorf("expected %d actions, got %d", len(expectedActions), len(actions))
	}
}

func TestRetiresPods(t *testing.T) {
	cluster := makeCluster()

	ownerRefs := []metav1.OwnerReference{*metav1.NewControllerRef(&cluster, daskv1alpha1.SchemeGroupVersion.WithKind("Cluster"))}
	kubeObjects := []runtime.Object{
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "foo-scheduler", Namespace: "bar", OwnerReferences: ownerRefs},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "tcp-comm", Port: 8786}}},
		},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "foo-scheduler", Namespace: "bar", OwnerReferences: ownerRefs}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       "bar",
			Labels:          clusterLabels(&cluster, "worker"),
			OwnerReferences: ownerRefs,
		}},
	}
	objects := []runtime.Object{&cluster}
	f := newFixture(t, objects, kubeObjects)

	_, ctx := ktesting.NewTestContext(t)
	controller := f.newController(ctx, objects, kubeObjects)

	if err := controller.syncHandler(ctx, getKey(&cluster, t)); err != nil {
		t.Fatalf("Error syncing cluster: %v", err)
	}

	expectedActions := []k8stesting.Action{}
	actions := filterActions(f.kubeclient.Actions())
	if len(actions) != len(expectedActions) {
		t.Errorf("expected %d actions, got %d", len(expectedActions), len(actions))
	}

	expectedDaskActions := []k8stesting.Action{
		k8stesting.NewPatchAction(
			schema.GroupVersionResource{Group: "dask.charmtx.com", Resource: "clusters"},
			cluster.Namespace,
			cluster.Name,
			types.ApplyPatchType,
			[]byte(""), // TODO: Check patch contents
		),
		k8stesting.NewPatchAction(
			schema.GroupVersionResource{Group: "dask.charmtx.com", Resource: "clusters"},
			cluster.Namespace,
			cluster.Name,
			types.ApplyPatchType,
			[]byte(""), // TODO: Check patch contents
		),
	}
	daskActions := filterActions(f.client.Actions())
	if len(daskActions) != len(expectedDaskActions) {
		t.Errorf("expected %d dask actions, got %d", len(expectedDaskActions), len(daskActions))
	}

	for i, daskAction := range daskActions {
		expectedDaskAction := expectedDaskActions[i]
		if !(expectedDaskAction.Matches(daskAction.GetVerb(), daskAction.GetResource().Resource) && daskAction.GetSubresource() == expectedDaskAction.GetSubresource()) {
			t.Errorf("dask action %v does not match expected dask action %v", daskAction, expectedDaskAction)
		}
	}

	expectedApiCalls := [][]daskv1alpha1.RetiredWorker{{{Id: "foo"}}}
	apiCalls := f.schedulerclient.events
	if len(expectedApiCalls) != len(apiCalls) {
		t.Errorf("expected %d actions, got %d", len(expectedApiCalls), len(apiCalls))
	}
}

func TestRepeatRetiresPods(t *testing.T) {
	cluster := makeCluster()
	cluster.Status = daskv1alpha1.ClusterStatus{
		Workers: daskv1alpha1.WorkerStatus{
			Retiring: []daskv1alpha1.RetiredWorker{{Id: "foo"}}},
	}

	ownerRefs := []metav1.OwnerReference{*metav1.NewControllerRef(&cluster, daskv1alpha1.SchemeGroupVersion.WithKind("Cluster"))}
	kubeObjects := []runtime.Object{
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "foo-scheduler", Namespace: "bar", OwnerReferences: ownerRefs},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "tcp-comm", Port: 8786}}},
		},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "foo-scheduler", Namespace: "bar", OwnerReferences: ownerRefs}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       "bar",
			Labels:          clusterLabels(&cluster, "worker"),
			OwnerReferences: ownerRefs,
		}},
	}
	objects := []runtime.Object{&cluster}
	f := newFixture(t, objects, kubeObjects)

	_, ctx := ktesting.NewTestContext(t)
	controller := f.newController(ctx, objects, kubeObjects)

	if err := controller.syncHandler(ctx, getKey(&cluster, t)); err != nil {
		t.Fatalf("Error syncing cluster: %v", err)
	}

	expectedActions := []k8stesting.Action{}
	actions := filterActions(f.kubeclient.Actions())
	if len(actions) != len(expectedActions) {
		t.Errorf("expected %d actions, got %d", len(expectedActions), len(actions))
	}

	expectedDaskActions := []k8stesting.Action{
		k8stesting.NewPatchAction(
			schema.GroupVersionResource{Group: "dask.charmtx.com", Resource: "clusters"},
			cluster.Namespace,
			cluster.Name,
			types.ApplyPatchType,
			[]byte(""), // TODO: Check patch contents
		),
	}
	daskActions := filterActions(f.client.Actions())
	if len(daskActions) != len(expectedDaskActions) {
		t.Errorf("expected %d dask actions, got %d", len(expectedDaskActions), len(daskActions))
	}

	for i, daskAction := range daskActions {
		expectedDaskAction := expectedDaskActions[i]
		if !(expectedDaskAction.Matches(daskAction.GetVerb(), daskAction.GetResource().Resource) && daskAction.GetSubresource() == expectedDaskAction.GetSubresource()) {
			t.Errorf("dask action %v does not match expected dask action %v", daskAction, expectedDaskAction)
		}
	}

	// Pod was already retired, so there should be no more calls to the retire function.
	expectedApiCalls := [][]daskv1alpha1.RetiredWorker{}
	apiCalls := f.schedulerclient.events
	if len(expectedApiCalls) != len(apiCalls) {
		t.Errorf("expected %d actions, got %d", len(expectedApiCalls), len(apiCalls))
	}
}
