// Command queue-operator runs the Queue CRD reconciler described in
// design.md §2a/§2c (shard-aware control plane: creates shard topics,
// performs capacity-driven shard splits, drains closed shards, and
// publishes queue metadata + the shard map to Redis for the message plane).
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kmsvcv1 "github.com/rockliang/kafka-management-service/apis/kmsvc/v1"
	"github.com/rockliang/kafka-management-service/internal/kafka"
	"github.com/rockliang/kafka-management-service/internal/operator"
)

func main() {
	ctrl.SetLogger(zap.New())

	brokers := splitCSV(getEnv("KMSVC_KAFKA_BROKERS", "localhost:9092"))
	redisAddr := getEnv("KMSVC_REDIS_ADDR", "localhost:6379")
	redisPassword := os.Getenv("KMSVC_REDIS_PASSWORD")
	redisDB := 0
	if v := os.Getenv("KMSVC_REDIS_DB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			exitf("invalid KMSVC_REDIS_DB: %v", err)
		}
		redisDB = n
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		exitf("registering client-go scheme: %v", err)
	}
	if err := kmsvcv1.AddToScheme(scheme); err != nil {
		exitf("registering kmsvc scheme: %v", err)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme})
	if err != nil {
		exitf("creating manager: %v", err)
	}

	admin, err := kafka.NewAdmin(brokers)
	if err != nil {
		exitf("creating kafka admin: %v", err)
	}

	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr, Password: redisPassword, DB: redisDB})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		exitf("connecting to redis at %s: %v", redisAddr, err)
	}

	reconciler := &operator.QueueReconciler{
		Client: mgr.GetClient(),
		Admin:  admin,
		Redis:  rdb,
		Now:    time.Now,
	}

	err = ctrl.NewControllerManagedBy(mgr).
		For(&kmsvcv1.Queue{}).
		Complete(reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
			if err := reconciler.Reconcile(ctx, req.Name); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, nil
		}))
	if err != nil {
		exitf("setting up controller: %v", err)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		exitf("running manager: %v", err)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
