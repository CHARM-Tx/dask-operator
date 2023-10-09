package main

import (
	"fmt"

	"github.com/CHARM-Tx/dask-operator/pkg/apis/dask"
	daskv1alpha1 "github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Return the value of overrides, concatenated with those values from template
// where none exists in overrides with the same key.
func replaceByName[T any](template, overrides []T, keyFn func(T) string) []T {
	templatesByKey := make(map[string]T, len(template))
	for _, v := range template {
		templatesByKey[keyFn(v)] = v
	}
	for _, v := range overrides {
		delete(templatesByKey, keyFn(v))
	}

	template = make([]T, 0, len(templatesByKey))
	for _, v := range templatesByKey {
		template = append(template, v)
	}
	return append(overrides, template...)
}

// Get a reference to the object from a list where keyFn is true.
func getByKey[T interface{}, K comparable](values []T, keyFn func(T) K, key K) *T {
	for i, v := range values {
		if keyFn(v) == key {
			return &values[i]
		}
	}
	return nil
}

func clusterLabels(cluster *daskv1alpha1.Cluster, role string) map[string]string {
	clusterName := cluster.ObjectMeta.Name
	return map[string]string{
		fmt.Sprintf("%s/cluster", dask.GroupName): clusterName,
		fmt.Sprintf("%s/role", dask.GroupName):    role,
	}
}

func addProbes(container *corev1.Container) {
	probeTemplate := corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("http-dashboard"), Path: "/health"},
		},
	}
	if container.ReadinessProbe == nil {
		probe := probeTemplate.DeepCopy()
		probe.InitialDelaySeconds = 5
		probe.PeriodSeconds = 10
		container.ReadinessProbe = probe
	}
	if container.LivenessProbe == nil {
		probe := probeTemplate.DeepCopy()
		probe.InitialDelaySeconds = 15
		probe.PeriodSeconds = 20
		container.LivenessProbe = probe
	}
}

func buildSchedulerDeployment(name string, cluster *daskv1alpha1.Cluster) (*appsv1.Deployment, error) {
	labels := clusterLabels(cluster, "scheduler")

	podTemplate := cluster.Spec.Scheduler.Template.DeepCopy()
	if podTemplate.Labels == nil {
		podTemplate.Labels = labels
	} else {
		for k, v := range labels {
			podTemplate.Labels[k] = v
		}
	}
	schedulerContainer := getByKey(podTemplate.Spec.Containers, func(c corev1.Container) string { return c.Name }, "scheduler")
	if schedulerContainer == nil {
		return nil, fmt.Errorf("scheduler template has no container named 'scheduler'")
	}
	podPorts := []corev1.ContainerPort{
		{Name: "tcp-comm", ContainerPort: 8786, Protocol: "TCP"},
		{Name: "http-dashboard", ContainerPort: 8787, Protocol: "TCP"},
	}
	schedulerContainer.Ports = replaceByName(podPorts, schedulerContainer.Ports, func(p corev1.ContainerPort) string { return p.Name })
	podEnv := []corev1.EnvVar{
		{
			// The scheduler API is disabled by default, see https://github.com/dask/distributed/issues/6407
			Name:  "DASK_DISTRIBUTED__SCHEDULER__HTTP__ROUTES",
			Value: "['distributed.http.scheduler.api','distributed.http.health']",
		},
	}
	schedulerContainer.Env = replaceByName(podEnv, schedulerContainer.Env, func(p corev1.EnvVar) string { return p.Name })
	addProbes(schedulerContainer)

	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, daskv1alpha1.SchemeGroupVersion.WithKind("Cluster")),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: *podTemplate,
		},
	}

	return deployment, nil
}

func buildSchedulerService(name string, cluster *daskv1alpha1.Cluster) *corev1.Service {
	labels := clusterLabels(cluster, "scheduler")

	serviceSpec := cluster.Spec.Scheduler.Service.DeepCopy()
	if serviceSpec.Selector == nil {
		serviceSpec.Selector = labels
	} else {
		for k, v := range labels {
			serviceSpec.Selector[k] = v
		}
	}

	servicePorts := []corev1.ServicePort{
		{Name: "tcp-comm", Port: 8786, TargetPort: intstr.FromString("tcp-comm"), Protocol: "TCP"},
		{Name: "http-dashboard", Port: 8787, TargetPort: intstr.FromString("http-dashboard"), Protocol: "TCP"},
	}
	serviceSpec.Ports = replaceByName(servicePorts, serviceSpec.Ports, func(p corev1.ServicePort) string { return p.Name })
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, daskv1alpha1.SchemeGroupVersion.WithKind("Cluster")),
			},
		},
		Spec: *serviceSpec,
	}
}

func buildWorkerPod(name string, scheduler *corev1.Service, cluster *daskv1alpha1.Cluster) (*corev1.Pod, error) {
	schedulerPort := getByKey(scheduler.Spec.Ports, func(p corev1.ServicePort) string { return p.Name }, "tcp-comm")
	if schedulerPort == nil {
		return nil, fmt.Errorf("scheduler service has no port \"tcp-comm\"")
	}
	podTemplate := cluster.Spec.Worker.Template.DeepCopy()
	podTemplate.ObjectMeta.GenerateName = fmt.Sprintf("%s-", name)
	if podTemplate.ObjectMeta.Labels == nil {
		podTemplate.ObjectMeta.Labels = clusterLabels(cluster, "worker")
	} else {
		for k, v := range clusterLabels(cluster, "worker") {
			podTemplate.ObjectMeta.Labels[k] = v
		}
	}
	podTemplate.ObjectMeta.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(cluster, daskv1alpha1.SchemeGroupVersion.WithKind("Cluster")),
	}

	workerContainer := getByKey(podTemplate.Spec.Containers, func(c corev1.Container) string { return c.Name }, "worker")
	if workerContainer == nil {
		return nil, fmt.Errorf("worker template has no container named 'worker'")
	}
	podPorts := []corev1.ContainerPort{
		{Name: "http-dashboard", ContainerPort: 8787, Protocol: "TCP"},
	}
	workerContainer.Ports = replaceByName(podPorts, workerContainer.Ports, func(p corev1.ContainerPort) string { return p.Name })
	podEnv := []corev1.EnvVar{
		{
			Name:      "DASK_WORKER_NAME",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
		},
		{
			Name:  "DASK_SCHEDULER_ADDRESS",
			Value: fmt.Sprintf("%s.%s.svc:%d", scheduler.Name, cluster.Namespace, schedulerPort.Port),
		},
	}
	workerContainer.Env = replaceByName(podEnv, workerContainer.Env, func(p corev1.EnvVar) string { return p.Name })
	addProbes(workerContainer)

	pod := &corev1.Pod{ObjectMeta: podTemplate.ObjectMeta, Spec: podTemplate.Spec}
	return pod, nil
}
