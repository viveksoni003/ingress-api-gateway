// Package logger builds the application's structured Zap logger and provides
// request-scoped context helpers (request_id / trace_id) so every log line
// across the request and worker lifecycle can be correlated.
package logger

import (
	"context"

	"github.com/viveksoni003/ingress-api-gateway/internal/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New constructs a *zap.Logger from configuration. JSON format is used for
// production (machine-parseable, ships to CloudWatch/Loki); console format is
// friendlier for local development.
func New(cfg config.LogConfig) (*zap.Logger, error) {
	level := zap.NewAtomicLevelAt(parseLevel(cfg.Level))

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "ts"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encCfg.EncodeDuration = zapcore.MillisDurationEncoder

	zcfg := zap.Config{
		Level:            level,
		Development:      cfg.Format != "json",
		Encoding:         encoding(cfg.Format),
		EncoderConfig:    encCfg,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}
	return zcfg.Build()
}

func encoding(format string) string {
	if format == "console" {
		return "console"
	}
	return "json"
}

func parseLevel(s string) zapcore.Level {
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return zapcore.InfoLevel
	}
	return l
}

// --- request-scoped context helpers ---------------------------------------

type ctxKey int

const (
	requestIDKey ctxKey = iota
	traceIDKey
)

// WithIDs stores the request id and trace id on the context.
func WithIDs(ctx context.Context, requestID, traceID string) context.Context {
	ctx = context.WithValue(ctx, requestIDKey, requestID)
	ctx = context.WithValue(ctx, traceIDKey, traceID)
	return ctx
}

// RequestID returns the request id stored on the context (or "").
func RequestID(ctx context.Context) string { return stringFrom(ctx, requestIDKey) }

// TraceID returns the trace id stored on the context (or "").
func TraceID(ctx context.Context) string { return stringFrom(ctx, traceIDKey) }

func stringFrom(ctx context.Context, k ctxKey) string {
	if v, ok := ctx.Value(k).(string); ok {
		return v
	}
	return ""
}

// FromContext returns a child logger pre-populated with the request_id and
// trace_id found on the context, so callers get correlation for free.
func FromContext(ctx context.Context, base *zap.Logger) *zap.Logger {
	return base.With(
		zap.String("request_id", RequestID(ctx)),
		zap.String("trace_id", TraceID(ctx)),
	)
}
