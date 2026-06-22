// Command server runs the kafkamgmt.v1 message-plane gRPC+REST API
// (design.md §1, §9): config load, Kafka/Redis client init, gRPC server +
// grpc-gateway mux sharing one auth interceptor, and per-queue reaper
// goroutines for redelivery/DLQ sweep (design.md §5).
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kafkamgmtv1 "forgejo.riotpiao.homelab.com/rock/kmsvc-proto/gen/kafkamgmt/v1"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/rockliang/kafka-management-service/internal/api/handlers"
	"github.com/rockliang/kafka-management-service/internal/api/interceptors"
	"github.com/rockliang/kafka-management-service/internal/auth"
	"github.com/rockliang/kafka-management-service/internal/config"
	"github.com/rockliang/kafka-management-service/internal/core/queue"
	"github.com/rockliang/kafka-management-service/internal/core/reaper"
	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

// queueDiscoveryInterval is how often the server rescans Redis for newly
// reconciled queues to start a reaper goroutine for (design.md §5) — queue
// lifecycle isn't exposed over gRPC, so this is the server's only signal.
const queueDiscoveryInterval = 10 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("kmsvc-server: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	rdb := kmsvcredis.NewClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer rdb.Close()

	admin, err := kafka.NewAdmin(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer admin.Close()

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	validator, err := auth.NewValidator(ctx, cfg.AuthentikIssuerURL, cfg.AuthentikAudience)
	if err != nil {
		return err
	}

	router := &queue.ShardRouter{Redis: rdb}

	svc := &handlers.QueueService{
		Redis:  rdb,
		Router: router,
		Send: &queue.SendMessageService{
			Redis:       rdb,
			Producer:    producer,
			Router:      router,
			DedupWindow: time.Duration(cfg.DedupWindowSeconds) * time.Second,
		},
		Delete:     &queue.DeleteMessageService{Redis: rdb, Committer: admin},
		Visibility: &queue.ChangeVisibilityService{Redis: rdb},
		Consumers:  &handlers.ConsumerRegistry{Brokers: cfg.KafkaBrokers, Router: router},
	}
	defer svc.Consumers.Close()

	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(interceptors.UnaryServerInterceptor(validator)),
		grpc.ChainStreamInterceptor(interceptors.StreamServerInterceptor(validator)),
	)
	kafkamgmtv1.RegisterQueueServiceServer(grpcSrv, svc)

	mux := runtime.NewServeMux()
	if err := kafkamgmtv1.RegisterQueueServiceHandlerServer(ctx, mux, svc); err != nil {
		return err
	}
	httpSrv := &http.Server{Addr: cfg.HTTPListenAddr, Handler: mux}

	r := &reaper.Reaper{
		Redis: rdb,
		DLQSender: &queue.SendMessageService{
			Redis:       rdb,
			Producer:    producer,
			Router:      router,
			DedupWindow: time.Duration(cfg.DedupWindowSeconds) * time.Second,
		},
	}
	go runReaperDiscovery(ctx, rdb, r)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		lis, err := net.Listen("tcp", cfg.GRPCListenAddr)
		if err != nil {
			log.Printf("grpc listen %s: %v", cfg.GRPCListenAddr, err)
			return
		}
		log.Printf("grpc listening on %s", cfg.GRPCListenAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		log.Printf("http listening on %s", cfg.HTTPListenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()

	wg.Wait()
	return nil
}

// runReaperDiscovery periodically scans for known queues and ensures each
// has a running reaper sweep goroutine. A queue is only ever added, never
// removed mid-run: if it's deleted, the next sweep finds no queue meta and
// is a harmless no-op (design.md §5's Sweep already handles this).
func runReaperDiscovery(ctx context.Context, rdb *goredis.Client, r *reaper.Reaper) {
	started := make(map[string]bool)
	ticker := time.NewTicker(queueDiscoveryInterval)
	defer ticker.Stop()

	discover := func() {
		names, err := kmsvcredis.ListQueueNames(ctx, rdb)
		if err != nil {
			log.Printf("reaper discovery: %v", err)
			return
		}
		for _, name := range names {
			if started[name] {
				continue
			}
			started[name] = true
			go r.Run(ctx, name, func(err error) {
				log.Printf("reaper sweep %s: %v", name, err)
			})
		}
	}

	discover()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			discover()
		}
	}
}
