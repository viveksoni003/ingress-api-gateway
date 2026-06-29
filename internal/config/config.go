// Package config loads all runtime configuration from environment variables
// with safe defaults, so the binary is 12-factor friendly and container ready.
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully-resolved application configuration.
type Config struct {
	Env     string // dev | staging | prod
	Server  ServerConfig
	Redis   RedisConfig
	DB      DBConfig
	Worker  WorkerConfig
	Limits  LimitConfig
	Security SecurityConfig
	Log     LogConfig
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
	MaxBodyBytes    int64
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// DBConfig holds PostgreSQL connection settings.
type DBConfig struct {
	URL         string
	MaxConns    int32
	MinConns    int32
	MaxConnLife time.Duration
}

// WorkerConfig controls the background worker pool.
type WorkerConfig struct {
	Count       int
	PollTimeout time.Duration // BLPOP timeout
	MaxRetries  int
	RetryBase   time.Duration
	RetryMax    time.Duration
}

// LimitConfig controls rate limiting and backpressure thresholds.
type LimitConfig struct {
	GlobalRPS      float64
	GlobalBurst    int
	RouteRPS       float64
	RouteBurst     int
	QueueMaxDepth  int64         // backpressure threshold (load shedding -> 503)
	IdempotencyTTL time.Duration // how long an idempotency key is remembered
	QRDedupTTL     time.Duration // window for rejecting duplicate scans
}

// SecurityConfig holds auth/crypto secrets.
type SecurityConfig struct {
	JWTSecret      string
	JWTIssuer      string
	PaymentHMACKey string
}

// LogConfig controls logging output.
type LogConfig struct {
	Level  string // debug | info | warn | error
	Format string // json | console
}

// Load reads configuration from the environment. It never fails; missing
// values fall back to development-friendly defaults. Validate() should be
// called in production to assert that secrets were overridden.
func Load() *Config {
	return &Config{
		Env: getEnv("ENV", "dev"),
		Server: ServerConfig{
			Port:            getEnv("HTTP_PORT", "8080"),
			ReadTimeout:     getEnvDuration("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:    getEnvDuration("HTTP_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:     getEnvDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
			ShutdownTimeout: getEnvDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
			MaxBodyBytes:    getEnvInt64("MAX_BODY_BYTES", 1<<20), // 1 MiB
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvInt("REDIS_DB", 0),
		},
		DB: DBConfig{
			URL:         getEnv("DATABASE_URL", "postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable"),
			MaxConns:    int32(getEnvInt("DB_MAX_CONNS", 20)),
			MinConns:    int32(getEnvInt("DB_MIN_CONNS", 2)),
			MaxConnLife: getEnvDuration("DB_MAX_CONN_LIFETIME", 30*time.Minute),
		},
		Worker: WorkerConfig{
			Count:       getEnvInt("WORKER_COUNT", 8),
			PollTimeout: getEnvDuration("WORKER_POLL_TIMEOUT", 5*time.Second),
			MaxRetries:  getEnvInt("JOB_MAX_RETRIES", 5),
			RetryBase:   getEnvDuration("RETRY_BASE_DELAY", 500*time.Millisecond),
			RetryMax:    getEnvDuration("RETRY_MAX_DELAY", 30*time.Second),
		},
		Limits: LimitConfig{
			GlobalRPS:      getEnvFloat("GLOBAL_RATE_RPS", 2000),
			GlobalBurst:    getEnvInt("GLOBAL_RATE_BURST", 4000),
			RouteRPS:       getEnvFloat("ROUTE_RATE_RPS", 50),
			RouteBurst:     getEnvInt("ROUTE_RATE_BURST", 100),
			QueueMaxDepth:  getEnvInt64("QUEUE_MAX_DEPTH", 10000),
			IdempotencyTTL: getEnvDuration("IDEMPOTENCY_TTL", 24*time.Hour),
			QRDedupTTL:     getEnvDuration("QR_DEDUP_TTL", 10*time.Second),
		},
		Security: SecurityConfig{
			JWTSecret:      getEnv("JWT_SECRET", "dev-secret-change-me"),
			JWTIssuer:      getEnv("JWT_ISSUER", "ingress-api-gateway"),
			PaymentHMACKey: getEnv("PAYMENT_HMAC_SECRET", "whsec_dev_secret_change_me"),
		},
		Log: LogConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "json"),
		},
	}
}

// Validate asserts that production deployments do not run with insecure
// defaults. Call it from main after Load.
func (c *Config) Validate() error {
	if c.Env != "prod" {
		return nil
	}
	if c.Security.JWTSecret == "dev-secret-change-me" {
		return fmt.Errorf("JWT_SECRET must be set in production")
	}
	if c.Security.PaymentHMACKey == "whsec_dev_secret_change_me" {
		return fmt.Errorf("PAYMENT_HMAC_SECRET must be set in production")
	}
	if c.Worker.Count <= 0 {
		return fmt.Errorf("WORKER_COUNT must be > 0")
	}
	return nil
}

// LoadDotEnv loads KEY=VALUE pairs from a .env file into the process
// environment if (and only if) they are not already set. It is a no-op when
// the file is absent, so it is safe to call unconditionally in main.
func LoadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.Trim(strings.TrimSpace(val), `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
}

// --- typed env helpers -----------------------------------------------------

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvInt64(key string, def int64) int64 {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
