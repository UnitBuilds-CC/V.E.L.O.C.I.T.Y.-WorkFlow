// Package main is the entry point for the Velocity Workflow Operator.
//
// It sets up a controller-runtime manager, registers the VelocityWorkflow
// reconciler, and starts the control loop with leader election and health
// probes.
package main

import (
	"flag"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1alpha1 "github.com/velocity-workflow/operator/api/v1alpha1"
	"github.com/velocity-workflow/operator/controllers"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr          string
		healthProbeAddr      string
		enableLeaderElection bool
		velocityServerAddr   string
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080",
		"The address the metric endpoint binds to.")
	flag.StringVar(&healthProbeAddr, "health-probe-bind-address", ":8081",
		"The address the health probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager to ensure only one active controller.")
	flag.StringVar(&velocityServerAddr, "velocity-server-addr", "",
		"gRPC address of the Velocity server (defaults to VELOCITY_SERVER_URL env or cluster default).")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Resolve the velocity server address.
	if velocityServerAddr == "" {
		velocityServerAddr = controllers.VelocityServerAddress()
	}

	// ── Create the gRPC client ────────────────────────────────────────────────
	velClient, err := controllers.NewGRPCVelocityClient(velocityServerAddr)
	if err != nil {
		setupLog.Error(err, "unable to create velocity gRPC client")
		os.Exit(1)
	}

	// ── Build the manager ─────────────────────────────────────────────────────
	watchNamespace := os.Getenv("WATCH_NAMESPACE")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: healthProbeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "velocity-operator-leader-election",
		// Cache configuration — restrict to a single namespace if set.
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	// ── Register the reconciler ───────────────────────────────────────────────
	reconciler := controllers.NewVelocityWorkflowReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		velClient,
	)
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "VelocityWorkflow")
		os.Exit(1)
	}

	// ── Health checks ─────────────────────────────────────────────────────────
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// ── Start ─────────────────────────────────────────────────────────────────
	setupLog.Info("starting velocity-operator",
		"namespace", watchNamespace,
		"velocity-server", velocityServerAddr,
	)
	fmt.Fprintf(os.Stdout, "velocity-operator starting — metrics=%s health=%s\n",
		metricsAddr, healthProbeAddr)

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
