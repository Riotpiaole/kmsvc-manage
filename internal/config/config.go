package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config is the env/flag-driven runtime configuration shared by the
// message-plane server and the queue-operator (design.md §10).
type Config struct {
	KafkaBrokers []string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	AuthentikIssuerURL string
	AuthentikAudience  string

	GRPCListenAddr string
	HTTPListenAddr string

	MaxMessageBodyBytes int

	DedupWindowSeconds int
}

// Load reads configuration from environment variables, applying the
// defaults documented in design.md.
func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers:        splitCSV(getEnv("KMSVC_KAFKA_BROKERS", "localhost:9092")),
		RedisAddr:           getEnv("KMSVC_REDIS_ADDR", "localhost:6379"),
		RedisPassword:       os.Getenv("KMSVC_REDIS_PASSWORD"),
		AuthentikIssuerURL:  os.Getenv("KMSVC_AUTHENTIK_ISSUER_URL"),
		AuthentikAudience:   os.Getenv("KMSVC_AUTHENTIK_AUDIENCE"),
		GRPCListenAddr:      getEnv("KMSVC_GRPC_LISTEN_ADDR", ":9090"),
		HTTPListenAddr:      getEnv("KMSVC_HTTP_LISTEN_ADDR", ":8080"),
		MaxMessageBodyBytes: 256 * 1024, // 256KB cap, design.md §6
		DedupWindowSeconds:  300,        // 5 minutes, design.md §4
	}

	if v := os.Getenv("KMSVC_REDIS_DB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid KMSVC_REDIS_DB: %w", err)
		}
		cfg.RedisDB = n
	}

	if cfg.AuthentikIssuerURL == "" {
		return nil, fmt.Errorf("KMSVC_AUTHENTIK_ISSUER_URL is required")
	}
	if cfg.AuthentikAudience == "" {
		return nil, fmt.Errorf("KMSVC_AUTHENTIK_AUDIENCE is required")
	}

	return cfg, nil
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
