package operator

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kmsvcv1 "github.com/rockliang/kafka-management-service/apis/kmsvc/v1"
)

func newTemporalWorkerTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := kmsvcv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kmsvc scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	return scheme
}

func newTemporalWorkerTestReconciler(t *testing.T, objs ...client.Object) (*TemporalWorkerReconciler, client.Client) {
	t.Helper()
	scheme := newTemporalWorkerTestScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kmsvcv1.TemporalWorker{}).
		WithObjects(objs...).
		Build()
	return &TemporalWorkerReconciler{Client: cl}, cl
}

func baseTemporalWorker(name, namespace string) *kmsvcv1.TemporalWorker {
	replicas := int32(2)
	return &kmsvcv1.TemporalWorker{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: kmsvcv1.TemporalWorkerSpec{
			Namespace: "default",
			Image:     "story-crater-backend:v1.0.0",
			Replicas:  &replicas,
		},
	}
}

func TestTemporalWorkerReconcileCreatesDeployment(t *testing.T) {
	worker := baseTemporalWorker("worker-default", "temporal")
	r, cl := newTemporalWorkerTestReconciler(t, worker)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "temporal", "worker-default"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var deploy appsv1.Deployment
	if err := cl.Get(ctx, client.ObjectKey{Name: "worker-default", Namespace: "temporal"}, &deploy); err != nil {
		t.Fatalf("expected Deployment to be created: %v", err)
	}
	if *deploy.Spec.Replicas != 2 {
		t.Errorf("replicas = %d, want 2", *deploy.Spec.Replicas)
	}
	if deploy.Spec.Template.Spec.Containers[0].Image != "story-crater-backend:v1.0.0" {
		t.Errorf("image = %q, want story-crater-backend:v1.0.0", deploy.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestTemporalWorkerReconcileInjectsEnvVars(t *testing.T) {
	worker := baseTemporalWorker("worker-default", "temporal")
	r, cl := newTemporalWorkerTestReconciler(t, worker)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "temporal", "worker-default"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var deploy appsv1.Deployment
	if err := cl.Get(ctx, client.ObjectKey{Name: "worker-default", Namespace: "temporal"}, &deploy); err != nil {
		t.Fatalf("get Deployment: %v", err)
	}

	envVars := deploy.Spec.Template.Spec.Containers[0].Env
	envMap := make(map[string]string)
	for _, ev := range envVars {
		envMap[ev.Name] = ev.Value
	}

	if envMap["TEMPORAL_FRONTEND_ADDRESS"] != "temporal-frontend.temporal.svc.cluster.local:7233" {
		t.Errorf("TEMPORAL_FRONTEND_ADDRESS = %q, want temporal-frontend.temporal.svc.cluster.local:7233", envMap["TEMPORAL_FRONTEND_ADDRESS"])
	}
	if envMap["TEMPORAL_NAMESPACE"] != "default" {
		t.Errorf("TEMPORAL_NAMESPACE = %q, want default", envMap["TEMPORAL_NAMESPACE"])
	}
	if envMap["TEMPORAL_TASK_QUEUE"] != "worker-default" {
		t.Errorf("TEMPORAL_TASK_QUEUE = %q, want worker-default", envMap["TEMPORAL_TASK_QUEUE"])
	}
}

func TestTemporalWorkerReconcileUpdateDeployment(t *testing.T) {
	replicas := int32(2)
	worker := baseTemporalWorker("worker-default", "temporal")
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-default", Namespace: "temporal"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":       "temporal-worker",
					"app.kubernetes.io/instance":   "worker-default",
					"app.kubernetes.io/managed-by": "kmsvc-temporal-operator",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":       "temporal-worker",
						"app.kubernetes.io/instance":   "worker-default",
						"app.kubernetes.io/managed-by": "kmsvc-temporal-operator",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "worker",
							Image: "story-crater-backend:v0.9.0",
						},
					},
				},
			},
		},
	}

	r, cl := newTemporalWorkerTestReconciler(t, worker, deploy)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "temporal", "worker-default"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var updated appsv1.Deployment
	if err := cl.Get(ctx, client.ObjectKey{Name: "worker-default", Namespace: "temporal"}, &updated); err != nil {
		t.Fatalf("get Deployment: %v", err)
	}

	if updated.Spec.Template.Spec.Containers[0].Image != "story-crater-backend:v1.0.0" {
		t.Errorf("image updated to %q, want story-crater-backend:v1.0.0", updated.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestTemporalWorkerReconcileUpdatesStatus(t *testing.T) {
	worker := baseTemporalWorker("worker-default", "temporal")
	r, cl := newTemporalWorkerTestReconciler(t, worker)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "temporal", "worker-default"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var updated kmsvcv1.TemporalWorker
	if err := cl.Get(ctx, client.ObjectKey{Name: "worker-default", Namespace: "temporal"}, &updated); err != nil {
		t.Fatalf("get TemporalWorker: %v", err)
	}

	if updated.Status.Phase != kmsvcv1.TemporalWorkerPhasePending {
		t.Errorf("phase = %v, want Pending", updated.Status.Phase)
	}
	if updated.Status.Replicas != 2 {
		t.Errorf("status.replicas = %d, want 2", updated.Status.Replicas)
	}
}

func TestTemporalWorkerReconcileDeleteHandlesMarkedForDeletion(t *testing.T) {
	worker := baseTemporalWorker("worker-default", "temporal")
	now := metav1.Now()
	worker.ObjectMeta.DeletionTimestamp = &now
	worker.ObjectMeta.Finalizers = []string{temporalWorkerFinalizerName}

	r, _ := newTemporalWorkerTestReconciler(t, worker)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "temporal", "worker-default"); err != nil {
		t.Fatalf("Reconcile delete should not error: %v", err)
	}
}
