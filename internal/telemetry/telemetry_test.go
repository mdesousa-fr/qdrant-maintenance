package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/testhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	traces "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func cleanEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"LOG_LEVEL", "OTEL_SDK_DISABLED", "OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER", "OTEL_LOGS_EXPORTER", "OTEL_TRACES_SAMPLER"} {
		t.Setenv(key, "")
	}
	for _, signal := range []string{"TRACES", "METRICS", "LOGS"} {
		for _, setting := range []string{"ENDPOINT", "PROTOCOL", "HEADERS", "COMPRESSION"} {
			t.Setenv("OTEL_EXPORTER_OTLP_"+signal+"_"+setting, "")
		}
	}
	tp, mp, prop := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	eh := otel.GetErrorHandler()
	t.Cleanup(func() {
		otel.SetTracerProvider(tp)
		otel.SetMeterProvider(mp)
		otel.SetTextMapPropagator(prop)
		otel.SetErrorHandler(eh)
	})
}
func TestShutdownExportsAllSignalsAndCorrelatesLogs(t *testing.T) {
	cleanEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector.test")
	t.Setenv("OTEL_SERVICE_NAME", "maintenance-test")
	var local bytes.Buffer
	var mu sync.Mutex
	payloads := map[string][]byte{}
	server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		mu.Lock()
		payloads[r.URL.Path] = b
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	logger, shutdown, err := start(context.Background(), "test", &local, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	commandCtx, cancelCommand := context.WithCancel(context.Background())
	ctx, finish := Operation(commandCtx, "backup")
	id := trace.SpanFromContext(ctx).SpanContext().TraceID()
	logger.InfoContext(ctx, "backup complete", "key", "docs/node/s.snapshot")
	Bytes(ctx, 17)
	Deleted(ctx)
	cancelCommand()
	finish(context.Canceled)
	// Shutdown uses a fresh context even when the command context was cancelled.
	flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := shutdown(flushCtx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	tr := &traces.ExportTraceServiceRequest{}
	if err := proto.Unmarshal(payloads["/v1/traces"], tr); err != nil || len(tr.ResourceSpans) != 1 {
		t.Fatalf("traces: %+v %v", tr, err)
	}
	lg := &logs.ExportLogsServiceRequest{}
	if err := proto.Unmarshal(payloads["/v1/logs"], lg); err != nil || len(lg.ResourceLogs) != 1 {
		t.Fatalf("logs: %+v %v", lg, err)
	}
	record := lg.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if !bytes.Equal(record.TraceId, id[:]) {
		t.Fatal("log is not correlated to trace")
	}
	mt := &metrics.ExportMetricsServiceRequest{}
	if err := proto.Unmarshal(payloads["/v1/metrics"], mt); err != nil || len(mt.ResourceMetrics) != 1 {
		t.Fatalf("metrics: %+v %v", mt, err)
	}
	names := map[string]bool{}
	for _, scope := range mt.ResourceMetrics[0].ScopeMetrics {
		for _, metric := range scope.Metrics {
			names[metric.Name] = true
		}
	}
	for _, name := range []string{"qdrant_maintenance.operations", "qdrant_maintenance.operation.duration", "qdrant_maintenance.backup.bytes", "qdrant_maintenance.retention.deleted"} {
		if !names[name] {
			t.Errorf("missing metric %s", name)
		}
	}
	if !bytes.Contains(local.Bytes(), []byte("backup complete")) {
		t.Fatal("missing stderr log")
	}
}
func TestOfflineAndDisabledTelemetry(t *testing.T) {
	for _, disabled := range []string{"", "true"} {
		t.Run("disabled="+disabled, func(t *testing.T) {
			cleanEnv(t)
			t.Setenv("OTEL_SDK_DISABLED", disabled)
			if disabled == "true" {
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://unreachable.test")
			}
			logger, shutdown, err := start(context.Background(), "test", io.Discard, &http.Client{Transport: failTransport{t}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, finish := Operation(context.Background(), "backup")
			logger.InfoContext(ctx, "offline")
			finish(nil)
			if err := shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type failTransport struct{ t *testing.T }

func (f failTransport) RoundTrip(*http.Request) (*http.Response, error) {
	f.t.Error("unexpected telemetry HTTP call")
	return nil, errors.New("unexpected HTTP")
}

func TestLogLevelFiltersLocalAndOTLP(t *testing.T) {
	for _, tc := range []struct {
		setting string
		want    int
	}{{"", 3}, {"DEBUG", 4}, {"info", 3}, {"WARN", 2}, {"ERROR", 1}} {
		t.Run("level="+tc.setting, func(t *testing.T) {
			cleanEnv(t)
			t.Setenv("LOG_LEVEL", tc.setting)
			t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://collector.test/v1/logs")
			var local bytes.Buffer
			var mu sync.Mutex
			records := 0
			server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				req := &logs.ExportLogsServiceRequest{}
				if err := proto.Unmarshal(data, req); err != nil {
					t.Error(err)
				}
				mu.Lock()
				for _, resource := range req.ResourceLogs {
					for _, scope := range resource.ScopeLogs {
						records += len(scope.LogRecords)
					}
				}
				mu.Unlock()
				w.Header().Set("Content-Type", "application/x-protobuf")
			}))
			logger, shutdown, err := start(context.Background(), "test", &local, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			logger = logger.With("component", "test").WithGroup("details")
			for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
				logger.Log(context.Background(), level, level.String())
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if got := bytes.Count(local.Bytes(), []byte("\n")); got != tc.want {
				t.Fatalf("stderr records=%d want %d: %s", got, tc.want, local.String())
			}
			mu.Lock()
			defer mu.Unlock()
			if records != tc.want {
				t.Fatalf("OTLP records=%d want %d", records, tc.want)
			}
		})
	}
}
func TestInvalidLogLevelRejected(t *testing.T) {
	cleanEnv(t)
	t.Setenv("LOG_LEVEL", "verbose")
	_, shutdown, err := start(context.Background(), "test", io.Discard, nil)
	if err == nil {
		t.Fatal("invalid level accepted")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExporterDiagnosticIsOneJSONRecord(t *testing.T) {
	cleanEnv(t)
	var output bytes.Buffer
	_, shutdown, err := start(context.Background(), "test", &output, nil)
	if err != nil {
		t.Fatal(err)
	}
	otel.Handle(errors.New("export failed\nretry later"))
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatalf("invalid JSON: %s", output.String())
	}
	if record["level"] != "ERROR" || record["error"] != "export failed\nretry later" {
		t.Fatal(record)
	}
	if bytes.Count(output.Bytes(), []byte("\n")) != 1 {
		t.Fatal("diagnostic spans multiple lines")
	}
}
