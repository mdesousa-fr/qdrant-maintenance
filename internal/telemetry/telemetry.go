// Package telemetry owns the short-lived CLI's OpenTelemetry lifecycle.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	logSDK "go.opentelemetry.io/otel/sdk/log"
	metricSDK "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	traceSDK "go.opentelemetry.io/otel/sdk/trace"
)

const scope = "github.com/mdesousa-fr/qdrant-maintenance"

// Start exports configured signals over OTLP HTTP/protobuf. Without an endpoint,
// commands remain usable offline. Each signal can be disabled with its exporter=none.
func Start(ctx context.Context, version string, stderr io.Writer) (*slog.Logger, func(context.Context) error, error) {
	return start(ctx, version, stderr, nil)
}

func start(ctx context.Context, version string, stderr io.Writer, httpClient *http.Client) (*slog.Logger, func(context.Context) error, error) {
	local := slog.NewJSONHandler(stderr, nil)
	shutdowns := []func(context.Context) error{}
	shutdown := func(ctx context.Context) error {
		var errs []error
		for _, fn := range shutdowns {
			errs = append(errs, fn(ctx))
		}
		return errors.Join(errs...)
	}
	logger := slog.New(local)
	level := slog.LevelInfo
	switch strings.ToUpper(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "", "INFO":
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		return logger, shutdown, fmt.Errorf("LOG_LEVEL must be DEBUG, INFO, WARN, or ERROR")
	}
	local = slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: level})
	logger = slog.New(local)
	// Exporter diagnostics stay local to avoid recursive OTLP logging failures.
	otel.SetLogger(logr.FromSlogHandler(local))
	diagnosticLogger := logger
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		diagnosticLogger.Error("OpenTelemetry diagnostic", "event", "telemetry.diagnostic", "error", err)
	}))
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		return logger, shutdown, nil
	}
	envResource, err := resource.New(ctx, resource.WithFromEnv())
	if err != nil {
		return logger, shutdown, err
	}
	res, err := resource.Merge(resource.NewSchemaless(attribute.String("service.name", "qdrant-maintenance"), attribute.String("service.version", version)), envResource)
	if err != nil {
		return logger, shutdown, err
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	for _, signal := range []string{"TRACES", "METRICS", "LOGS"} {
		exporter := os.Getenv("OTEL_" + signal + "_EXPORTER")
		if exporter == "none" {
			continue
		}
		if exporter != "" && exporter != "otlp" {
			return logger, shutdown, fmt.Errorf("OTEL_%s_EXPORTER supports otlp or none", signal)
		}
		if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_"+signal+"_ENDPOINT") == "" && exporter == "" {
			continue
		}
		protocol := os.Getenv("OTEL_EXPORTER_OTLP_" + signal + "_PROTOCOL")
		if protocol == "" {
			protocol = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
		}
		if protocol != "" && protocol != "http/protobuf" {
			return logger, shutdown, fmt.Errorf("OTLP %s protocol must be http/protobuf", signal)
		}
		switch signal {
		case "TRACES":
			opts := []otlptracehttp.Option{}
			if httpClient != nil {
				opts = append(opts, otlptracehttp.WithHTTPClient(httpClient))
			}
			e, err := otlptracehttp.New(ctx, opts...)
			if err != nil {
				return logger, shutdown, err
			}
			p := traceSDK.NewTracerProvider(traceSDK.WithResource(res), traceSDK.WithBatcher(e))
			otel.SetTracerProvider(p)
			shutdowns = append(shutdowns, p.Shutdown)
		case "METRICS":
			opts := []otlpmetrichttp.Option{}
			if httpClient != nil {
				opts = append(opts, otlpmetrichttp.WithHTTPClient(httpClient))
			}
			e, err := otlpmetrichttp.New(ctx, opts...)
			if err != nil {
				return logger, shutdown, err
			}
			p := metricSDK.NewMeterProvider(metricSDK.WithResource(res), metricSDK.WithReader(metricSDK.NewPeriodicReader(e)))
			otel.SetMeterProvider(p)
			shutdowns = append(shutdowns, p.Shutdown)
		case "LOGS":
			opts := []otlploghttp.Option{}
			if httpClient != nil {
				opts = append(opts, otlploghttp.WithHTTPClient(httpClient))
			}
			e, err := otlploghttp.New(ctx, opts...)
			if err != nil {
				return logger, shutdown, err
			}
			p := logSDK.NewLoggerProvider(logSDK.WithResource(res), logSDK.WithProcessor(logSDK.NewBatchProcessor(e)))
			shutdowns = append(shutdowns, p.Shutdown)
			logger = slog.New(fanout{local: local, remote: otelslog.NewHandler(scope, otelslog.WithLoggerProvider(p)), level: level})
		}
	}
	return logger, shutdown, nil
}

type fanout struct {
	level  slog.Level
	local  slog.Handler
	remote slog.Handler
}

func (h fanout) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.level && (h.local.Enabled(ctx, l) || h.remote.Enabled(ctx, l))
}
func (h fanout) Handle(ctx context.Context, r slog.Record) error {
	if r.Level < h.level {
		return nil
	}
	var errs []error
	for _, handler := range []slog.Handler{h.local, h.remote} {
		if handler.Enabled(ctx, r.Level) {
			errs = append(errs, handler.Handle(ctx, r.Clone()))
		}
	}
	return errors.Join(errs...)
}
func (h fanout) WithAttrs(a []slog.Attr) slog.Handler {
	return fanout{local: h.local.WithAttrs(a), remote: h.remote.WithAttrs(a), level: h.level}
}
func (h fanout) WithGroup(g string) slog.Handler {
	return fanout{local: h.local.WithGroup(g), remote: h.remote.WithGroup(g), level: h.level}
}

// Operation creates a span and low-cardinality metrics. Finish must be called
// before provider shutdown, including when the operation fails or is cancelled.
func Operation(ctx context.Context, name string) (context.Context, func(error)) {
	ctx, span := otel.Tracer(scope).Start(ctx, name)
	start := time.Now()
	return ctx, func(err error) {
		outcome := "success"
		if err != nil {
			outcome = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, "operation failed")
		}
		opts := metric.WithAttributes(attribute.String("operation", name), attribute.String("outcome", outcome))
		m := otel.Meter(scope)
		count, _ := m.Int64Counter("qdrant_maintenance.operations")
		duration, _ := m.Float64Histogram("qdrant_maintenance.operation.duration", metric.WithUnit("s"))
		count.Add(ctx, 1, opts)
		duration.Record(ctx, time.Since(start).Seconds(), opts)
		span.End()
	}
}
func Bytes(ctx context.Context, n int64) {
	c, _ := otel.Meter(scope).Int64Counter("qdrant_maintenance.backup.bytes", metric.WithUnit("By"))
	c.Add(ctx, n)
}
func Deleted(ctx context.Context) {
	c, _ := otel.Meter(scope).Int64Counter("qdrant_maintenance.retention.deleted")
	c.Add(ctx, 1)
}

func BackupAge(ctx context.Context, seconds float64) {
	g, _ := otel.Meter(scope).Float64Gauge("qdrant_maintenance.backup.age", metric.WithUnit("s"))
	g.Record(ctx, seconds)
}
