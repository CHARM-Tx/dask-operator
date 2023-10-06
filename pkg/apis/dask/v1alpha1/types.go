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
	Replicas int32              `json:"replicas,omitempty"`
	Template v1.PodTemplateSpec `json:"template,omitempty"`
}

type ClusterSpec struct {
	Scheduler SchedulerSpec `json:"scheduler,omitempty"`
	Worker    WorkerSpec    `json:"worker,omitempty"`
}

type SchedulerStatus struct {
	Address string `json:"address,omitempty"`
}

type RetiredWorker struct {
	Id string `json:"id,omitEmpty"`
}

type WorkerStatus struct {
	Count int32 `json:"count,omitempty"`
	// +listType=map
	// +listMapKey=Id
	Retiring []RetiredWorker `json:"retiring,omitempty"`
}

type ClusterStatus struct {
	Scheduler SchedulerStatus `json:"scheduler,omitempty"`
	Workers   WorkerStatus    `json:"workers,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Cluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterSpec   `json:"spec,omitempty"`
	Status ClusterStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Cluster `json:"items"`
}
