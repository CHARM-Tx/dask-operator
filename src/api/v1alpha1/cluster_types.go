// +kubebuilder:object:generate=true
// +groupName=dask.charmtx.com
package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type SchedulerSpec struct {
	Template v1.PodTemplateSpec `json:"template,omitempty"`
	Service  v1.ServiceSpec     `json:"service,omitempty"`
}

type WorkerSpec struct {
	Replicas    int32              `json:"replicas,omitempty"`
	MinReplicas int32              `json:"minReplicas,omitempty"`
	MaxReplicas int32              `json:"maxReplicas,omitempty"`
	Template    v1.PodTemplateSpec `json:"template,omitempty"`
}

type ClusterSpec struct {
	Scheduler SchedulerSpec `json:"scheduler,omitempty"`
	Worker    WorkerSpec    `json:"worker,omitempty"`
}

type SchedulerStatus struct {
	Address string `json:"address,omitempty"`
}

type WorkerStatus struct {
	Count int32 `json:"count,omitempty"`
}

type ClusterStatus struct {
	Scheduler SchedulerStatus `json:"scheduler,omitempty"`
	Workers   WorkerStatus    `json:"workers,omitempty"`
}

// +kubebuilder:subresource:status
type Cluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterSpec   `json:"spec,omitempty"`
	Status ClusterStatus `json:"status,omitempty"`
}
