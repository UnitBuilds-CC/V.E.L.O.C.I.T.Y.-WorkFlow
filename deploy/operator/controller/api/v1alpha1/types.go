// Package v1alpha1 contains API types for the VelocityWorkflow CRD.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the API group and version for VelocityWorkflow resources.
var GroupVersion = schema.GroupVersion{Group: "workflow.velocity.io", Version: "v1alpha1"}

// SchemeBuilder is used to register types with the runtime scheme.
var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

// AddToScheme applies all stored functions to the scheme.
func AddToScheme(s *runtime.Scheme) error {
	return SchemeBuilder.AddToScheme(s)
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&VelocityWorkflow{},
		&VelocityWorkflowList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

// ── Spec ──────────────────────────────────────────────────────────────────────

// VelocityWorkflowSpec defines the desired state of a VelocityWorkflow.
type VelocityWorkflowSpec struct {
	// WorkflowType is the type/name of the workflow to execute.
	WorkflowType string `json:"workflowType"`

	// Namespace is the Velocity namespace for workflow isolation.
	Namespace string `json:"namespace"`

	// Steps is an ordered list of workflow steps.
	Steps []WorkflowStep `json:"steps,omitempty"`

	// TaskQueue is the task queue for activity dispatch.
	TaskQueue string `json:"taskQueue"`

	// RetryPolicy defines the default retry policy for the workflow.
	RetryPolicy *RetryPolicy `json:"retryPolicy,omitempty"`

	// ExecutionTimeout is the overall workflow execution timeout (e.g. "30m").
	ExecutionTimeout string `json:"executionTimeout,omitempty"`

	// RunTimeout is the timeout for a single workflow run (e.g. "10m").
	RunTimeout string `json:"runTimeout,omitempty"`
}

// WorkflowStep describes a single step in a workflow.
type WorkflowStep struct {
	// Name is a human-readable step name.
	Name string `json:"name"`

	// Activity is the activity type to execute.
	Activity string `json:"activity"`

	// Timeout is the step timeout (e.g. "30s", "5m").
	Timeout string `json:"timeout,omitempty"`

	// RetryPolicy is an optional per-step retry policy.
	RetryPolicy *RetryPolicy `json:"retryPolicy,omitempty"`

	// Input is arbitrary JSON input for the step.
	Input *runtime.RawExtension `json:"input,omitempty"`
}

// RetryPolicy controls retry behaviour for a workflow or step.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of retry attempts (1-100).
	MaxAttempts int32 `json:"maxAttempts,omitempty"`

	// InitialInterval is the initial backoff interval (e.g. "1s").
	InitialInterval string `json:"initialInterval,omitempty"`

	// BackoffCoefficient is the multiplier applied after each retry.
	BackoffCoefficient float64 `json:"backoffCoefficient,omitempty"`
}

// ── Status ────────────────────────────────────────────────────────────────────

// VelocityWorkflowStatus defines the observed state of a VelocityWorkflow.
type VelocityWorkflowStatus struct {
	// State is the current workflow state.
	// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed;Cancelled;TimedOut
	State string `json:"state,omitempty"`

	// CurrentStep is the name of the currently executing step.
	CurrentStep string `json:"currentStep,omitempty"`

	// StartedAt is the timestamp when the workflow started executing.
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is the timestamp when the workflow completed.
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// WorkflowId is the internal workflow execution ID.
	WorkflowId string `json:"workflowId,omitempty"`

	// RunId is the internal workflow run ID.
	RunId string `json:"runId,omitempty"`

	// Message is a human-readable message about the current state.
	Message string `json:"message,omitempty"`

	// StepResults holds results from completed steps.
	StepResults []StepResult `json:"stepResults,omitempty"`

	// Conditions represent the latest available observations of an object's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the last observed generation of the resource.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// StepResult holds the output of a completed workflow step.
type StepResult struct {
	StepName    string              `json:"stepName"`
	Status      string              `json:"status"`
	Output      *runtime.RawExtension `json:"output,omitempty"`
	CompletedAt *metav1.Time        `json:"completedAt,omitempty"`
}

// ── Root types ────────────────────────────────────────────────────────────────

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Workflow Type",type=string,JSONPath=`.spec.workflowType`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Current Step",type=string,JSONPath=`.status.currentStep`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// VelocityWorkflow is the Schema for the velocityworkflows API.
type VelocityWorkflow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VelocityWorkflowSpec   `json:"spec,omitempty"`
	Status VelocityWorkflowStatus `json:"status,omitempty"`
}

// DeepCopyObject implements runtime.Object.
func (in *VelocityWorkflow) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// DeepCopy returns a deep copy of VelocityWorkflow.
func (in *VelocityWorkflow) DeepCopy() *VelocityWorkflow {
	if in == nil {
		return nil
	}
	out := new(VelocityWorkflow)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies all properties into another VelocityWorkflow.
func (in *VelocityWorkflow) DeepCopyInto(out *VelocityWorkflow) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopyInto copies spec fields.
func (in *VelocityWorkflowSpec) DeepCopyInto(out *VelocityWorkflowSpec) {
	*out = *in
	if in.Steps != nil {
		out.Steps = make([]WorkflowStep, len(in.Steps))
		for i := range in.Steps {
			in.Steps[i].DeepCopyInto(&out.Steps[i])
		}
	}
	if in.RetryPolicy != nil {
		out.RetryPolicy = new(RetryPolicy)
		*out.RetryPolicy = *in.RetryPolicy
	}
}

// DeepCopyInto copies step fields.
func (in *WorkflowStep) DeepCopyInto(out *WorkflowStep) {
	*out = *in
	if in.RetryPolicy != nil {
		out.RetryPolicy = new(RetryPolicy)
		*out.RetryPolicy = *in.RetryPolicy
	}
	if in.Input != nil {
		out.Input = in.Input.DeepCopy()
	}
}

// DeepCopyInto copies status fields.
func (in *VelocityWorkflowStatus) DeepCopyInto(out *VelocityWorkflowStatus) {
	*out = *in
	if in.StartedAt != nil {
		out.StartedAt = in.StartedAt.DeepCopy()
	}
	if in.CompletedAt != nil {
		out.CompletedAt = in.CompletedAt.DeepCopy()
	}
	if in.StepResults != nil {
		out.StepResults = make([]StepResult, len(in.StepResults))
		for i := range in.StepResults {
			in.StepResults[i].DeepCopyInto(&out.StepResults[i])
		}
	}
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

// DeepCopyInto copies step result fields.
func (in *StepResult) DeepCopyInto(out *StepResult) {
	*out = *in
	if in.Output != nil {
		out.Output = in.Output.DeepCopy()
	}
	if in.CompletedAt != nil {
		out.CompletedAt = in.CompletedAt.DeepCopy()
	}
}

// +kubebuilder:object:root=true

// VelocityWorkflowList contains a list of VelocityWorkflow resources.
type VelocityWorkflowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VelocityWorkflow `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (in *VelocityWorkflowList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// DeepCopy returns a deep copy of VelocityWorkflowList.
func (in *VelocityWorkflowList) DeepCopy() *VelocityWorkflowList {
	if in == nil {
		return nil
	}
	out := new(VelocityWorkflowList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies all properties into another VelocityWorkflowList.
func (in *VelocityWorkflowList) DeepCopyInto(out *VelocityWorkflowList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]VelocityWorkflow, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
