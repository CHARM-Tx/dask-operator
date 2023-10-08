package main

import (
	"testing"

	daskv1alpha1 "github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkerTemplate(t *testing.T) {
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
				Replicas: 1,
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

	worker, err := buildWorkerPod("foo", "foo-scheduler", &cluster)
	if err != nil {
		t.Fatalf("failed to create pod: %s", err)
	}

	envVars := make(map[string]string, len(worker.Spec.Containers[0].Env))
	for _, env := range worker.Spec.Containers[0].Env {
		envVars[env.Name] = env.Value
	}
	if envVars["DASK_SCHEDULER_ADDRESS"] != "foo-scheduler.bar.svc" {
		t.Errorf("incorrect scheduler address: %s", envVars["DASK_SCHEDULER_ADDRESS"])
	}
}
