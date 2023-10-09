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

func TestMain(m *testing.M) {
	namespace := envconf.RandomName("dask-operator", 16)
	cfg := envconf.NewWithKubeConfig("./kubeconfig.yaml")
	testenv = env.NewWithConfig(cfg)

	testenv.Setup(
		envfuncs.SetupCRDs("../../config/crd", "*"),
		envfuncs.CreateNamespace(namespace),
	)

	testenv.Finish(
		envfuncs.DeleteNamespace(namespace),
		envfuncs.TeardownCRDs("../../config/crd", "*"),
	)
	os.Exit(testenv.Run(m))
}
func deployOperator(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	client, err := cfg.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"app": "dask-controller"}
	serviceAccount := corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "dask-controller", Namespace: cfg.Namespace()},
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "dask-controller", Namespace: cfg.Namespace()},
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
						Args:            []string{fmt.Sprintf("--namespace=%s", cfg.Namespace())},
						ImagePullPolicy: "IfNotPresent",
					}},
				},
			},
		},
	}
	role := rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "dask-controller", Namespace: cfg.Namespace()},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods", "services"}, Verbs: []string{"*"}},
			{APIGroups: []string{"apps"}, Resources: []string{"deployments"}, Verbs: []string{"*"}},
			{APIGroups: []string{"dask.charmtx.com"}, Resources: []string{"clusters"}, Verbs: []string{"*"}},
		},
	}
	roleBinding := rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "dask-controller", Namespace: cfg.Namespace()},
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
	cluster := daskv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: cfg.Namespace()},
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
			Worker: daskv1alpha1.WorkerSpec{
				Replicas: 1,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:            "worker",
								Image:           "ghcr.io/dask/dask:latest",
								Command:         []string{"dask", "worker"},
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
	return ctx
}

func TestKind(t *testing.T) {
	feat := features.New("Create Dask cluster").
		WithSetup("Deploy controller", deployOperator).
		WithSetup("Create cluster", makeCluster).
		Assess("Scheduler exists", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			schedulerDeployment := appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "foo-scheduler", Namespace: cfg.Namespace()},
			}
			wait.For(conditions.New(client.Resources()).ResourceMatch(
				&schedulerDeployment,
				func(object k8s.Object) bool {
					deployment := object.(*appsv1.Deployment)
					return deployment.Status.Replicas == 1
				},
			), wait.WithTimeout(time.Second*30))
			return ctx
		}).
		Assess("Worker exists", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			workerPods := corev1.PodList{}
			wait.For(conditions.New(client.Resources()).ResourceListMatchN(
				&workerPods, 1,
				func(object k8s.Object) bool {
					pod := object.(*corev1.Pod)
					return pod.Status.Phase == "Running"
				},
				resources.WithLabelSelector(labels.FormatLabels(map[string]string{
					"dask.charmtx.com/cluster": "foo",
					"dask.charmtx.com/role":    "worker",
				})),
			), wait.WithTimeout(time.Second*30))
			return ctx
		}).Feature()

	testenv.Test(t, feat)
}
