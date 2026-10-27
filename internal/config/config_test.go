package config

import (
	"os"
	"strconv"
	"testing"
)

func unsetEnv(t *testing.T, key string) {
	t.Helper()
	initialContent, defined := os.LookupEnv(key)
	t.Cleanup(func() {
		if defined {
			os.Setenv(key, initialContent)
		} else {
			os.Unsetenv(key)
		}
	})
	os.Unsetenv(key)
}

func TestLoadConfig(t *testing.T) {
	expected := struct {
		QdrantURL         string
		QdrantAPIKey      string
		RetentionKeepLast int
	}{
		QdrantURL:         "http://localhost:6333",
		QdrantAPIKey:      "qdrant-secret",
		RetentionKeepLast: defaultKeepLast,
	}

	t.Setenv("QDRANT_URL", expected.QdrantURL)
	t.Setenv("QDRANT_API_KEY", expected.QdrantAPIKey)
	unsetEnv(t, "RETENTION_KEEP_LAST")

	cfg, err := Load()

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if cfg.Qdrant.URL != expected.QdrantURL {
		t.Errorf("QdrantURL = %q, want %q", cfg.Qdrant.URL, expected.QdrantURL)
	}

	if cfg.Qdrant.APIKey != expected.QdrantAPIKey {
		t.Errorf("QdrantAPIKey = %q, want %q", cfg.Qdrant.APIKey, expected.QdrantAPIKey)
	}

	if cfg.Retention.KeepLast != expected.RetentionKeepLast {
		t.Errorf("RetentionKeepLast = %d, want %d", cfg.Retention.KeepLast, expected.RetentionKeepLast)
	}
}

func TestLoadConfigWithRetentionKeepLast(t *testing.T) {
	expected := 10

	t.Setenv("QDRANT_URL", "http://localhost:6333")
	t.Setenv("QDRANT_API_KEY", "qdrant-secret")
	t.Setenv("RETENTION_KEEP_LAST", strconv.Itoa(expected))

	cfg, err := Load()

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if cfg.Retention.KeepLast != expected {
		t.Errorf("RetentionKeepLast = %d, want %d", cfg.Retention.KeepLast, expected)
	}
}

func TestLoadConfigWithInvalidRetentionKeepLast(t *testing.T) {
	t.Setenv("QDRANT_URL", "http://localhost:6333")
	t.Setenv("QDRANT_API_KEY", "qdrant-secret")
	t.Setenv("RETENTION_KEEP_LAST", "abc")

	_, err := Load()

	if err == nil {
		t.Errorf("expected an error, got nil")
	}
}

func TestDistributedNodeConfiguration(t *testing.T) {
	t.Setenv("QDRANT_URL", "")
	t.Setenv("QDRANT_TIMEOUT", "")
	for _, tc := range []struct {
		name, value string
		ok          bool
	}{
		{"valid", `{"node-1":"http://node-1:6333","node-0":"http://node-0:6333"}`, true},
		{"missing", "", false}, {"empty", `{}`, false}, {"null", `null`, false},
		{"invalid JSON", `{`, false}, {"bad ID", `{"../node":"http://node:6333"}`, false},
		{"bad URL", `{"node":"node:6333"}`, false},
		{"duplicates", `{"a":"http://node:6333","b":"http://node:6333/"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("QDRANT_NODES", tc.value)
			c, err := LoadQdrant(true)
			if (err == nil) != tc.ok {
				t.Fatalf("error=%v", err)
			}
			if tc.ok && (len(c.Nodes) != 2 || c.Nodes[0].ID != "node-0" || c.URL != "http://node-0:6333") {
				t.Fatal(c)
			}
		})
	}
}
func TestStorageDoesNotRequireQdrant(t *testing.T) {
	t.Setenv("QDRANT_URL", "")
	t.Setenv("QDRANT_API_KEY", "")
	t.Setenv("QDRANT_NODES", "")
	t.Setenv("S3_BUCKET", "backups")
	t.Setenv("S3_ENDPOINT", "")
	t.Setenv("S3_FORCE_PATH_STYLE", "")
	t.Setenv("S3_TIMEOUT", "")
	t.Setenv("AWS_REGION", "eu-west-1")
	c, err := LoadStorage()
	if err != nil {
		t.Fatal(err)
	}
	if c.ForcePathStyle || c.Endpoint != "" || c.Bucket != "backups" {
		t.Fatal(c)
	}
	t.Setenv("S3_ENDPOINT", "http://s3.test:9000")
	c, err = LoadStorage()
	if err != nil || !c.ForcePathStyle {
		t.Fatalf("config=%+v error=%v", c, err)
	}
}
func TestInvalidSettings(t *testing.T) {
	t.Setenv("S3_BUCKET", "backups")
	t.Setenv("S3_ENDPOINT", "")
	t.Setenv("S3_FORCE_PATH_STYLE", "")
	t.Setenv("S3_TIMEOUT", "")
	for _, tc := range []struct{ key, value string }{
		{"S3_BUCKET", ""}, {"S3_ENDPOINT", "ftp://bad"}, {"S3_FORCE_PATH_STYLE", "maybe"}, {"S3_TIMEOUT", "0s"}, {"S3_TIMEOUT", "bad"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := LoadStorage(); err == nil {
				t.Fatal("invalid setting accepted")
			}
		})
	}
	for _, value := range []string{"-1", "bad"} {
		t.Run("retention="+value, func(t *testing.T) {
			t.Setenv("RETENTION_KEEP_LAST", value)
			if _, err := LoadRetention(); err == nil {
				t.Fatal("invalid retention accepted")
			}
		})
	}
}

func TestRetentionPolicyValidation(t *testing.T) {
	t.Setenv("RETENTION_KEEP_LAST", "5")
	for _, policy := range []string{"", "snapshot", "run", "invalid"} {
		t.Run(policy, func(t *testing.T) {
			t.Setenv("RETENTION_POLICY", policy)
			c, err := LoadRetention()
			if policy == "invalid" {
				if err == nil {
					t.Fatal("invalid policy accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := policy
			if want == "" {
				want = "snapshot"
			}
			if c.Policy != want {
				t.Fatal(c)
			}
		})
	}
}
