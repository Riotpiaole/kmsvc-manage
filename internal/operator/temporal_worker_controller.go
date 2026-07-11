package operator

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	kmsvcv1 "github.com/rockliang/kafka-management-service/apis/kmsvc/v1"
)

const temporalWorkerFinalizerName = "kmsvc.io/temporal-worker"

// TemporalWorkerReconciler reconciles TemporalWorker objects by creating and
// managing corresponding Kubernetes Deployments.
type TemporalWorkerReconciler struct {
	Client client.Client
}

// Reconcile creates/updates/deletes a Deployment based on the TemporalWorker object.
func (r *TemporalWorkerReconciler) Reconcile(ctx context.Context, namespace, name string) error {
	logger := ctrllog.FromContext(ctx)
	var worker kmsvcv1.TemporalWorker
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &worker); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get temporalworker %s/%s: %w", namespace, name, err)
	}

	if !worker.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &worker)
	}

	if !controllerutil.ContainsFinalizer(&worker, temporalWorkerFinalizerName) {
		controllerutil.AddFinalizer(&worker, temporalWorkerFinalizerName)
		if err := r.Client.Update(ctx, &worker); err != nil {
			return fmt.Errorf("add finalizer: %w", err)
		}
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      worker.Name,
			Namespace: worker.Namespace,
		},
	}

	replicas := int32(1)
	if worker.Spec.Replicas != nil {
		replicas = *worker.Spec.Replicas
	}

	imagePullPolicy := corev1.PullIfNotPresent
	if worker.Spec.ImagePullPolicy != "" {
		imagePullPolicy = worker.Spec.ImagePullPolicy
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		if err := controllerutil.SetControllerReference(&worker, deployment, r.Client.Scheme()); err != nil {
			return fmt.Errorf("set controller reference: %w", err)
		}

		deployment.Spec.Replicas = &replicas
		deployment.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"app.kubernetes.io/name":       "temporal-worker",
				"app.kubernetes.io/instance":   worker.Name,
				"app.kubernetes.io/managed-by": "kmsvc-temporal-operator",
			},
		}
		deployment.Spec.Template.ObjectMeta.Labels = map[string]string{
			"app.kubernetes.io/name":       "temporal-worker",
			"app.kubernetes.io/instance":   worker.Name,
			"app.kubernetes.io/managed-by": "kmsvc-temporal-operator",
		}

		container := corev1.Container{
			Name:            "worker",
			Image:           worker.Spec.Image,
			ImagePullPolicy: imagePullPolicy,
			Env: []corev1.EnvVar{
				{
					Name:  "TEMPORAL_FRONTEND_ADDRESS",
					Value: "temporal-frontend.temporal.svc.cluster.local:7233",
				},
				{
					Name:  "TEMPORAL_NAMESPACE",
					Value: worker.Spec.Namespace,
				},
				{
					Name:  "TEMPORAL_TASK_QUEUE",
					Value: worker.Name,
				},
			},
		}

		if worker.Spec.Resources != nil {
			container.Resources = *worker.Spec.Resources
		}

		deployment.Spec.Template.Spec.Containers = []corev1.Container{container}

		if len(worker.Spec.NodeSelector) > 0 {
			deployment.Spec.Template.Spec.NodeSelector = worker.Spec.NodeSelector
		}
		if worker.Spec.Affinity != nil {
			deployment.Spec.Template.Spec.Affinity = worker.Spec.Affinity
		}
		if len(worker.Spec.Tolerations) > 0 {
			deployment.Spec.Template.Spec.Tolerations = worker.Spec.Tolerations
		}

		return nil
	}); err != nil {
		logger.Error(err, "failed to create or update deployment", "deployment", deployment.Name)
		worker.Status.Phase = kmsvcv1.TemporalWorkerPhaseFailed
		if err := r.Client.Status().Update(ctx, &worker); err != nil {
			return fmt.Errorf("update status failed: %w", err)
		}
		return err
	}

	worker.Status.Replicas = *deployment.Spec.Replicas
	worker.Status.ReadyReplicas = deployment.Status.ReadyReplicas
	if deployment.Status.ReadyReplicas == *deployment.Spec.Replicas {
		worker.Status.Phase = kmsvcv1.TemporalWorkerPhaseReady
	} else {
		worker.Status.Phase = kmsvcv1.TemporalWorkerPhasePending
	}

	if err := r.Client.Status().Update(ctx, &worker); err != nil {
		return fmt.Errorf("update status: %w", err)
	}

	return nil
}

func (r *TemporalWorkerReconciler) reconcileDelete(ctx context.Context, worker *kmsvcv1.TemporalWorker) error {
	if !controllerutil.ContainsFinalizer(worker, temporalWorkerFinalizerName) {
		return nil
	}

	controllerutil.RemoveFinalizer(worker, temporalWorkerFinalizerName)
	if err := r.Client.Update(ctx, worker); err != nil {
		return fmt.Errorf("remove finalizer: %w", err)
	}
	return nil
}
