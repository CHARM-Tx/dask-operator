package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	daskv1alpha1 "github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

var testenv env.Environment

type contextKey string

func TestMain(m *testing.M) {
	cfg := envconf.NewWithKubeConfig("./kubeconfig.yaml")
	testenv = env.NewWithConfig(cfg)

	testenv.Setup(envfuncs.SetupCRDs("../../config/crd", "*"))

	testenv.Finish(envfuncs.TeardownCRDs("../../config/crd", "*"))
	os.Exit(testenv.Run(m))
}

func createNamespace(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	namespaceName := envconf.RandomName("dask-operator", 20)
	client, err := cfg.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	namespace := corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespaceName}}
	if err := client.Resources().Create(ctx, &namespace); err != nil {
		t.Fatal(err)
	}

	return context.WithValue(ctx, contextKey("namespace"), &namespace)
}

func deleteNamespace(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	client, err := cfg.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	namespace := ctx.Value(contextKey("namespace")).(*corev1.Namespace)
	if err := client.Resources().Delete(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	return ctx
}

func deployOperator(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	client, err := cfg.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	namespace := ctx.Value(contextKey("namespace")).(*corev1.Namespace)

	labels := map[string]string{"app": "dask-controller"}
	serviceAccount := corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "dask-controller", Namespace: namespace.Name},
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "dask-controller", Namespace: namespace.Name},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Replicas: pointer.Int32(1),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccount.Name,
					Containers: []corev1.Container{{
						Name:            "operator",
						Image:           "dask-operator:latest",
						Args:            []string{fmt.Sprintf("--namespace=%s", namespace.Name)},
						ImagePullPolicy: "IfNotPresent",
					}},
				},
			},
		},
	}
	role := rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "dask-controller", Namespace: namespace.Name},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods", "services"}, Verbs: []string{"*"}},
			{APIGroups: []string{"apps"}, Resources: []string{"deployments"}, Verbs: []string{"*"}},
			{APIGroups: []string{"dask.charmtx.com"}, Resources: []string{"clusters", "clusters/status", "clusters/scale"}, Verbs: []string{"*"}},
		},
	}
	roleBinding := rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "dask-controller", Namespace: namespace.Name},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: serviceAccount.Name, Namespace: serviceAccount.Namespace}},
	}
	objects := []k8s.Object{&serviceAccount, &role, &roleBinding, &deployment}
	for _, obj := range objects {
		if err := client.Resources().Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	wait.For(conditions.New(client.Resources()).ResourceMatch(
		&deployment,
		func(object k8s.Object) bool {
			deployment := object.(*appsv1.Deployment)
			return deployment.Status.ReadyReplicas >= 1
		},
	), wait.WithTimeout(time.Second*10))
	return ctx
}

func makeCluster(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	r, err := resources.New(cfg.Client().RESTConfig())
	if err != nil {
		t.Fatal(err)
	}
	daskv1alpha1.AddToScheme(r.GetScheme())

	namespace := ctx.Value(contextKey("namespace")).(*corev1.Namespace)
	cluster := daskv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      envconf.RandomName("test", 10),
			Namespace: namespace.Name,
		},
		Spec: daskv1alpha1.ClusterSpec{
			Scheduler: daskv1alpha1.SchedulerSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:            "scheduler",
								Image:           "ghcr.io/dask/dask:latest",
								Command:         []string{"dask", "scheduler"},
								ImagePullPolicy: "IfNotPresent",
							},
						},
					},
				},
				Service: corev1.ServiceSpec{},
			},
			Workers: daskv1alpha1.WorkerSpec{
				Replicas: 1,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "worker",
								Image: "ghcr.io/dask/dask:latest",
								Command: []string{
									"dask", "worker", "--no-nanny",
									"--name=$(DASK_WORKER_NAME)",
									"--listen-address=tcp://$(DASK_WORKER_ADDRESS):$(DASK_WORKER_PORT)",
									"--dashboard-address=$(DASK_WORKER_ADDRESS):$(DASK_DASHBOARD_PORT)",
								},
								ImagePullPolicy: "IfNotPresent",
							},
						},
					},
				},
			},
		},
	}
	if err := r.Create(ctx, &cluster); err != nil {
		t.Fatal(err)
	}
	return context.WithValue(ctx, contextKey("cluster"), &cluster)
}

func workerReady(object k8s.Object) bool {
	pod := object.(*corev1.Pod)
	if pod.Status.Phase != "Running" {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == "Ready" && c.Status == "True" {
			return true
		}
	}
	return false
}

func TestKind(t *testing.T) {
	createsCluster := features.New("Create Dask cluster").
		WithSetup("Create namespace", createNamespace).
		WithTeardown("Delete namespace", deleteNamespace).
		WithSetup("Deploy controller", deployOperator).
		WithSetup("Create cluster", makeCluster).
		Assess("Scheduler exists", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			namespace := ctx.Value(contextKey("namespace")).(*corev1.Namespace)
			schedulerDeployments := appsv1.DeploymentList{}
			wait.For(conditions.New(client.Resources()).ResourceListMatchN(
				&schedulerDeployments, 1,
				func(object k8s.Object) bool {
					deployment := object.(*appsv1.Deployment)
					return deployment.Status.Replicas == 1
				},
				resources.WithFieldSelector(labels.FormatLabels(map[string]string{"metadata.namespace": namespace.Name})),
			), wait.WithTimeout(time.Second*30))
			return ctx
		}).
		Assess("Worker exists", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			namespace := ctx.Value(contextKey("namespace")).(*corev1.Namespace)
			cluster := ctx.Value(contextKey("cluster")).(*daskv1alpha1.Cluster)

			workerPods := corev1.PodList{}
			err = wait.For(conditions.New(client.Resources()).ResourceListMatchN(
				&workerPods, 1, workerReady,
				resources.WithLabelSelector(labels.FormatLabels(map[string]string{
					"dask.charmtx.com/cluster": cluster.Name,
					"dask.charmtx.com/role":    "worker",
				})),
				resources.WithFieldSelector(labels.FormatLabels(map[string]string{"metadata.namespace": namespace.Name})),
			), wait.WithTimeout(time.Second*60))

			if err != nil {
				t.Errorf("error waiting for worker pod: %s", err)
			}
			return ctx
		}).Feature()

	scalesDownCluster := features.New("Scales down Dask cluster").
		WithSetup("Create namespace", createNamespace).
		WithTeardown("Delete namespace", deleteNamespace).
		WithSetup("Deploy controller", deployOperator).
		WithSetup("Create cluster", makeCluster).
		Assess("Worker deleted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			r, err := resources.New(cfg.Client().RESTConfig())
			if err != nil {
				t.Fatal(err)
			}
			daskv1alpha1.AddToScheme(r.GetScheme())

			namespace := ctx.Value(contextKey("namespace")).(*corev1.Namespace)
			cluster := ctx.Value(contextKey("cluster")).(*daskv1alpha1.Cluster)

			workerPods := corev1.PodList{}
			err = wait.For(conditions.New(r).ResourceListMatchN(
				&workerPods, 1, workerReady,
				resources.WithLabelSelector(labels.FormatLabels(map[string]string{
					"dask.charmtx.com/cluster": cluster.Name,
					"dask.charmtx.com/role":    "worker",
				})),
				resources.WithFieldSelector(labels.FormatLabels(map[string]string{"metadata.namespace": namespace.Name})),
			), wait.WithTimeout(time.Second*120))
			if err != nil {
				t.Errorf("error waiting for worker pod: %s", err)
			}

			newCluster := &daskv1alpha1.Cluster{}
			if err := r.Get(ctx, cluster.Name, cluster.Namespace, newCluster); err != nil {
				t.Errorf("failed to get cluster for update: %s", err)
			}

			newCluster.Spec.Workers.Replicas = 0
			if err := r.Update(ctx, newCluster); err != nil {
				t.Fatalf("failed to scale down: %s", err)
			}

			err = wait.For(conditions.New(r).ResourceDeleted(&workerPods.Items[0]), wait.WithTimeout(time.Second*10))
			if err != nil {
				t.Errorf("error waiting for worker pod deletion: %s", err)
			}
			return ctx
		}).Feature()

	replacesPod := features.New("Replaces deleted pods").
		WithSetup("Create namespace", createNamespace).
		WithTeardown("Delete namespace", deleteNamespace).
		WithSetup("Deploy controller", deployOperator).
		WithSetup("Create cluster", makeCluster).
		WithSetup("Delete worker", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			r, err := resources.New(cfg.Client().RESTConfig())
			if err != nil {
				t.Fatal(err)
			}

			namespace := ctx.Value(contextKey("namespace")).(*corev1.Namespace)
			cluster := ctx.Value(contextKey("cluster")).(*daskv1alpha1.Cluster)
			workerPods := corev1.PodList{}
			err = wait.For(conditions.New(r).ResourceListMatchN(
				&workerPods, 1, workerReady,
				resources.WithLabelSelector(labels.FormatLabels(map[string]string{
					"dask.charmtx.com/cluster": cluster.Name,
					"dask.charmtx.com/role":    "worker",
				})),
				resources.WithFieldSelector(labels.FormatLabels(map[string]string{"metadata.namespace": namespace.Name})),
			))
			if err != nil {
				t.Errorf("error waiting for worker pod: %s", err)
			}

			err = r.Delete(ctx, &workerPods.Items[0])
			if err != nil {
				t.Errorf("error deleting pod: %s", err)
			}
			err = wait.For(conditions.New(r).ResourceDeleted(&workerPods.Items[0]), wait.WithTimeout(time.Second*10))
			if err != nil {
				t.Errorf("error waiting for worker pod deletion: %s", err)
			}

			return ctx
		}).
		Assess("Worker replaced", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			r, err := resources.New(cfg.Client().RESTConfig())
			if err != nil {
				t.Fatal(err)
			}

			namespace := ctx.Value(contextKey("namespace")).(*corev1.Namespace)
			cluster := ctx.Value(contextKey("cluster")).(*daskv1alpha1.Cluster)
			workerPods := corev1.PodList{}
			err = wait.For(conditions.New(r).ResourceListMatchN(
				&workerPods, 1, workerReady,
				resources.WithLabelSelector(labels.FormatLabels(map[string]string{
					"dask.charmtx.com/cluster": cluster.Name,
					"dask.charmtx.com/role":    "worker",
				})),
				resources.WithFieldSelector(labels.FormatLabels(map[string]string{"metadata.namespace": namespace.Name})),
			))
			if err != nil {
				t.Errorf("error waiting for worker pod: %s", err)
			}
			return ctx
		}).Feature()

	testenv.TestInParallel(t, createsCluster, scalesDownCluster, replacesPod)
}
