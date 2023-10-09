package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkerTemplate(t *testing.T) {
	cluster := makeCluster()
	scheduler := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo-scheduler", Namespace: "bar"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "tcp-comm", Port: 8786}}},
	}

	worker, err := buildWorkerPod("foo", &scheduler, &cluster)
	if err != nil {
		t.Fatalf("failed to create pod: %s", err)
	}

	envVars := make(map[string]string, len(worker.Spec.Containers[0].Env))
	for _, env := range worker.Spec.Containers[0].Env {
		envVars[env.Name] = env.Value
	}
	if envVars["DASK_SCHEDULER_ADDRESS"] != "foo-scheduler.bar.svc:8786" {
		t.Errorf("incorrect scheduler address: %s", envVars["DASK_SCHEDULER_ADDRESS"])
	}
}
