package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/aws/smithy-go/logging"
)

func TestSDKDiagnosticIsOneJSONRecord(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	sdkLogger(context.Background(), logger).Logf(logging.Warn, "warning: %s", "first line\nsecond line")
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatalf("invalid JSON: %s", output.String())
	}
	if record["level"] != "WARN" || record["message"] != "warning: first line\nsecond line" {
		t.Fatal(record)
	}
	if bytes.Count(output.Bytes(), []byte("\n")) != 1 {
		t.Fatal("warning spans multiple lines")
	}
}
