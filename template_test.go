package main

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSchedulerServiceTemplate(t *testing.T) {
	cluster := makeCluster()

	service := buildSchedulerService("foo-scheduler", &cluster)
	selector := map[string]string{
		"dask.charmtx.com/cluster": "foo",
		"dask.charmtx.com/role":    "scheduler",
	}
	if !reflect.DeepEqual(service.Spec.Selector, selector) {
		t.Errorf("Incorrect scheduler selector, got: %v", service.Spec.Selector)
	}
}

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
	if envVars["DASK_SCHEDULER_ADDRESS"] != "tcp://foo-scheduler.bar.svc:8786" {
		t.Errorf("incorrect scheduler address: %s", envVars["DASK_SCHEDULER_ADDRESS"])
	}
}
