package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TemporalWorkerPhase tracks the reconciliation state of a TemporalWorker.
type TemporalWorkerPhase string

const (
	TemporalWorkerPhasePending TemporalWorkerPhase = "Pending"
	TemporalWorkerPhaseReady   TemporalWorkerPhase = "Ready"
	TemporalWorkerPhaseFailed  TemporalWorkerPhase = "Failed"
)

// TemporalWorkerSpec defines the desired state of a TemporalWorker.
// One TemporalWorker per Temporal namespace handles all task queues in that namespace.
type TemporalWorkerSpec struct {
	// Namespace is the Temporal namespace this worker connects to.
	// The worker will process all task queues in this namespace.
	Namespace string `json:"namespace"`

	// Image is the worker container image (must include Temporal SDK and
	// activity/workflow registration logic for all task queues in the namespace).
	Image string `json:"image"`

	// ImagePullPolicy controls when the image is pulled.
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	// +kubebuilder:default=IfNotPresent
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Replicas is the desired number of worker Deployment replicas.
	// Scale horizontally to handle aggregate queue depth across all task queues in the namespace.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Resources defines CPU/memory requests and limits for each worker pod.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// NodeSelector for pod placement (e.g., preferring worker nodes).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity for advanced pod placement rules.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations for pod placement on tainted nodes.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// TemporalWorkerStatus defines the observed state of a TemporalWorker.
type TemporalWorkerStatus struct {
	// Phase is the current reconciliation phase.
	// +kubebuilder:validation:Enum=Pending;Ready;Failed
	Phase TemporalWorkerPhase `json:"phase,omitempty"`

	// Replicas is the current replica count from the underlying Deployment.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of ready replicas from the underlying Deployment.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Conditions hold detailed status information.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Desired",type=string,JSONPath=`.status.replicas`
// +kubebuilder:resource:shortName=worker;workers;tw

// TemporalWorker is auto-created when a Queue CRD has the `temporal.io/namespace` label.
// The controller creates a Deployment that runs Temporal worker containers listening
// to the Temporal namespace specified in the label.
type TemporalWorker struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TemporalWorkerSpec   `json:"spec,omitempty"`
	Status TemporalWorkerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TemporalWorkerList contains a list of TemporalWorker.
type TemporalWorkerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TemporalWorker `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TemporalWorker{}, &TemporalWorkerList{})
}

// DeepCopyInto copies the receiver, writing into out.
func (in *TemporalWorker) DeepCopyInto(out *TemporalWorker) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy returns a deep copy of the object.
func (in *TemporalWorker) DeepCopy() *TemporalWorker {
	if in == nil {
		return nil
	}
	out := new(TemporalWorker)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy as runtime.Object.
func (in *TemporalWorker) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// DeepCopyInto copies the receiver, writing into out.
func (in *TemporalWorkerSpec) DeepCopyInto(out *TemporalWorkerSpec) {
	*out = *in
	if in.Replicas != nil {
		in, out := &in.Replicas, &out.Replicas
		*out = new(int32)
		**out = **in
	}
	if in.Resources != nil {
		in, out := &in.Resources, &out.Resources
		*out = new(corev1.ResourceRequirements)
		(*in).DeepCopyInto(*out)
	}
	if in.NodeSelector != nil {
		in, out := &in.NodeSelector, &out.NodeSelector
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.Affinity != nil {
		in, out := &in.Affinity, &out.Affinity
		*out = new(corev1.Affinity)
		(*in).DeepCopyInto(*out)
	}
	if in.Tolerations != nil {
		in, out := &in.Tolerations, &out.Tolerations
		*out = make([]corev1.Toleration, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy returns a deep copy of the object.
func (in *TemporalWorkerSpec) DeepCopy() *TemporalWorkerSpec {
	if in == nil {
		return nil
	}
	out := new(TemporalWorkerSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver, writing into out.
func (in *TemporalWorkerStatus) DeepCopyInto(out *TemporalWorkerStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy returns a deep copy of the object.
func (in *TemporalWorkerStatus) DeepCopy() *TemporalWorkerStatus {
	if in == nil {
		return nil
	}
	out := new(TemporalWorkerStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver, writing into out.
func (in *TemporalWorkerList) DeepCopyInto(out *TemporalWorkerList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]TemporalWorker, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy returns a deep copy of the object.
func (in *TemporalWorkerList) DeepCopy() *TemporalWorkerList {
	if in == nil {
		return nil
	}
	out := new(TemporalWorkerList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy as runtime.Object.
func (in *TemporalWorkerList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}
