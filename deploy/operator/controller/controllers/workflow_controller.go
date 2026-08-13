// Package controllers implements the VelocityWorkflow reconciler.
package controllers

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/velocity-workflow/operator/api/v1alpha1"
)

const (
	finalizerName = "velocityworkflow.workflow.velocity.io/finalizer"

	// Requeue intervals.
	requeueInterval      = 10 * time.Second
	requeueErrorInterval = 30 * time.Second
)

// ── gRPC client interface ─────────────────────────────────────────────────────

// VelocityClient abstracts the gRPC calls to the velocity server.
type VelocityClient interface {
	StartWorkflow(ctx context.Context, workflowType, namespace, taskQueue string, steps []v1alpha1.WorkflowStep) (workflowId, runId string, err error)
	GetWorkflowStatus(ctx context.Context, workflowId, runId string) (state, currentStep string, err error)
	CancelWorkflow(ctx context.Context, workflowId, runId string) error
}

// grpcVelocityClient is the real gRPC implementation.
type grpcVelocityClient struct {
	conn *grpc.ClientConn
}

// NewGRPCVelocityClient creates a new gRPC client connected to the given address.
func NewGRPCVelocityClient(addr string) (VelocityClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to velocity server %s: %w", addr, err)
	}
	return &grpcVelocityClient{conn: conn}, nil
}

func (c *grpcVelocityClient) StartWorkflow(ctx context.Context, workflowType, namespace, taskQueue string, steps []v1alpha1.WorkflowStep) (string, string, error) {
	// In a real implementation this would call the velocity gRPC StartWorkflow RPC.
	// Placeholder: the server would return a workflowId and runId.
	_ = status.Convert // keep import alive
	return "", "", fmt.Errorf("gRPC StartWorkflow not yet wired to velocity server proto stubs")
}

func (c *grpcVelocityClient) GetWorkflowStatus(ctx context.Context, workflowId, runId string) (string, string, error) {
	return "", "", fmt.Errorf("gRPC GetWorkflowStatus not yet wired to velocity server proto stubs")
}

func (c *grpcVelocityClient) CancelWorkflow(ctx context.Context, workflowId, runId string) error {
	return fmt.Errorf("gRPC CancelWorkflow not yet wired to velocity server proto stubs")
}

// ── Reconciler ────────────────────────────────────────────────────────────────

// VelocityWorkflowReconciler reconciles VelocityWorkflow objects.
type VelocityWorkflowReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	vel    VelocityClient
}

// NewVelocityWorkflowReconciler returns a new reconciler.
func NewVelocityWorkflowReconciler(c client.Client, s *runtime.Scheme, v VelocityClient) *VelocityWorkflowReconciler {
	return &VelocityWorkflowReconciler{client: c, scheme: s, vel: v}
}

// SetupWithManager registers the reconciler with the controller manager.
func (r *VelocityWorkflowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.VelocityWorkflow{}).
		Complete(r)
}

// Reconcile is the main reconciliation loop.
func (r *VelocityWorkflowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("velocityworkflow", req.NamespacedName)

	// ── Fetch the CR ──────────────────────────────────────────────────────────
	wf := &v1alpha1.VelocityWorkflow{}
	if err := r.client.Get(ctx, req.NamespacedName, wf); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("VelocityWorkflow resource not found; ignoring")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "unable to fetch VelocityWorkflow")
		return ctrl.Result{}, err
	}

	// ── Handle deletion via finalizer ─────────────────────────────────────────
	if !wf.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, wf)
	}

	// Ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(wf, finalizerName) {
		controllerutil.AddFinalizer(wf, finalizerName)
		if err := r.client.Update(ctx, wf); err != nil {
			logger.Error(err, "failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// ── State machine ─────────────────────────────────────────────────────────
	switch wf.Status.State {
	case "", "Pending":
		return r.handlePending(ctx, wf)
	case "Running":
		return r.handleRunning(ctx, wf)
	case "Completed", "Failed", "Cancelled", "TimedOut":
		// Terminal states — nothing to do.
		logger.Info("workflow in terminal state", "state", wf.Status.State)
		return ctrl.Result{}, nil
	default:
		logger.Info("unknown state, treating as pending", "state", wf.Status.State)
		return r.handlePending(ctx, wf)
	}
}

// handlePending transitions a workflow from Pending → Running by calling the gRPC server.
func (r *VelocityWorkflowReconciler) handlePending(ctx context.Context, wf *v1alpha1.VelocityWorkflow) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("starting workflow", "type", wf.Spec.WorkflowType)

	workflowId, runId, err := r.vel.StartWorkflow(ctx,
		wf.Spec.WorkflowType,
		wf.Spec.Namespace,
		wf.Spec.TaskQueue,
		wf.Spec.Steps,
	)
	if err != nil {
		logger.Error(err, "failed to start workflow via gRPC")
		r.setCondition(wf, "Ready", "False", "StartFailed", err.Error())
		wf.Status.Message = fmt.Sprintf("failed to start: %v", err)
		if statusErr := r.client.Status().Update(ctx, wf); statusErr != nil {
			logger.Error(statusErr, "failed to update status after start failure")
		}
		return ctrl.Result{RequeueAfter: requeueErrorInterval}, nil
	}

	now := metav1.Now()
	wf.Status.State = "Running"
	wf.Status.WorkflowId = workflowId
	wf.Status.RunId = runId
	wf.Status.StartedAt = &now
	wf.Status.Message = "workflow started"
	r.setCondition(wf, "Ready", "True", "Started", "Workflow is running")

	if err := r.client.Status().Update(ctx, wf); err != nil {
		logger.Error(err, "failed to update status to Running")
		return ctrl.Result{}, err
	}

	logger.Info("workflow started", "workflowId", workflowId, "runId", runId)
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// handleRunning polls the gRPC server for workflow progress.
func (r *VelocityWorkflowReconciler) handleRunning(ctx context.Context, wf *v1alpha1.VelocityWorkflow) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	serverState, currentStep, err := r.vel.GetWorkflowStatus(ctx, wf.Status.WorkflowId, wf.Status.RunId)
	if err != nil {
		logger.Error(err, "failed to get workflow status from gRPC")
		wf.Status.Message = fmt.Sprintf("poll error: %v", err)
		if statusErr := r.client.Status().Update(ctx, wf); statusErr != nil {
			logger.Error(statusErr, "failed to update status after poll error")
		}
		return ctrl.Result{RequeueAfter: requeueErrorInterval}, nil
	}

	wf.Status.CurrentStep = currentStep
	wf.Status.ObservedGeneration = wf.Generation

	switch serverState {
	case "COMPLETED":
		now := metav1.Now()
		wf.Status.State = "Completed"
		wf.Status.CompletedAt = &now
		wf.Status.Message = "workflow completed successfully"
		r.setCondition(wf, "Ready", "True", "Completed", "Workflow completed")
		logger.Info("workflow completed")

	case "FAILED":
		now := metav1.Now()
		wf.Status.State = "Failed"
		wf.Status.CompletedAt = &now
		wf.Status.Message = "workflow failed"
		r.setCondition(wf, "Ready", "False", "Failed", "Workflow failed")
		logger.Info("workflow failed")

	case "CANCELLED":
		now := metav1.Now()
		wf.Status.State = "Cancelled"
		wf.Status.CompletedAt = &now
		wf.Status.Message = "workflow cancelled"
		r.setCondition(wf, "Ready", "False", "Cancelled", "Workflow cancelled")
		logger.Info("workflow cancelled")

	case "TIMED_OUT":
		now := metav1.Now()
		wf.Status.State = "TimedOut"
		wf.Status.CompletedAt = &now
		wf.Status.Message = "workflow timed out"
		r.setCondition(wf, "Ready", "False", "TimedOut", "Workflow timed out")
		logger.Info("workflow timed out")

	default:
		// Still running — update step and requeue.
		wf.Status.Message = fmt.Sprintf("running step: %s", currentStep)
	}

	if err := r.client.Status().Update(ctx, wf); err != nil {
		logger.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	// Requeue if still running.
	if wf.Status.State == "Running" {
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}
	return ctrl.Result{}, nil
}

// handleDeletion runs cleanup when the CR is being deleted.
func (r *VelocityWorkflowReconciler) handleDeletion(ctx context.Context, wf *v1alpha1.VelocityWorkflow) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(wf, finalizerName) {
		// Cancel the workflow on the server side if it is still running.
		if wf.Status.State == "Running" && wf.Status.WorkflowId != "" {
			logger.Info("cancelling workflow on server before deletion")
			if err := r.vel.CancelWorkflow(ctx, wf.Status.WorkflowId, wf.Status.RunId); err != nil {
				logger.Error(err, "failed to cancel workflow on server; proceeding with deletion")
			}
		}

		controllerutil.RemoveFinalizer(wf, finalizerName)
		if err := r.client.Update(ctx, wf); err != nil {
			logger.Error(err, "failed to remove finalizer")
			return ctrl.Result{}, err
		}
		logger.Info("finalizer removed; resource will be deleted")
	}
	return ctrl.Result{}, nil
}

// setCondition updates or appends a condition on the workflow status.
func (r *VelocityWorkflowReconciler) setCondition(wf *v1alpha1.VelocityWorkflow, condType, condStatus, reason, message string) {
	now := metav1.Now()
	for i, c := range wf.Status.Conditions {
		if c.Type == condType {
			if c.Status != metav1.ConditionStatus(condStatus) {
				wf.Status.Conditions[i].LastTransitionTime = now
			}
			wf.Status.Conditions[i].Status = metav1.ConditionStatus(condStatus)
			wf.Status.Conditions[i].Reason = reason
			wf.Status.Conditions[i].Message = message
			return
		}
	}
	wf.Status.Conditions = append(wf.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             metav1.ConditionStatus(condStatus),
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// VelocityServerAddress returns the gRPC address from the environment or a default.
func VelocityServerAddress() string {
	if addr := os.Getenv("VELOCITY_SERVER_URL"); addr != "" {
		return addr
	}
	return "velocity-server.velocity-system.svc.cluster.local:7234"
}

// Ensure corev1 is used (for Event creation in extended implementations).
var _ = corev1.Event{}
