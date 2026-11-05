package config

import "testing"

func TestParallelNodesPrecedence(t *testing.T) {
	for _, tc := range []struct {
		env      string
		value    int
		explicit bool
		want     int
		fail     bool
	}{
		{"", 1, false, 1, false}, {"3", 1, false, 3, false}, {"2", 4, true, 4, false},
		{"bad", 4, true, 4, false}, {"bad", 1, false, 0, true}, {"0", 1, false, 0, true},
		{"-1", 1, false, 0, true}, {"2", 0, true, 0, true}, {"2", -1, true, 0, true},
	} {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv("QDRANT_PARALLEL_NODES", tc.env)
			got, err := ParallelNodes(tc.value, tc.explicit)
			if (err != nil) != tc.fail || got != tc.want {
				t.Fatalf("got=%d err=%v", got, err)
			}
		})
	}
}
func TestMultipartConfiguration(t *testing.T) {
	t.Setenv("S3_BUCKET", "test")
	t.Setenv("S3_ENDPOINT", "")
	t.Setenv("S3_TIMEOUT", "")
	t.Setenv("S3_FORCE_PATH_STYLE", "")
	t.Setenv("S3_INSECURE_SKIP_VERIFY", "")
	for _, tc := range []struct {
		workers, part string
		wantWorkers   int
		wantPart      int64
		fail          bool
	}{
		{"", "", 2, 16, false}, {"4", "64", 4, 64, false}, {"1", "5", 1, 5, false}, {"2", "5120", 2, 5120, false},
		{"0", "16", 0, 0, true}, {"-1", "16", 0, 0, true}, {"bad", "16", 0, 0, true},
		{"2", "4", 0, 0, true}, {"2", "5121", 0, 0, true}, {"2", "0", 0, 0, true}, {"2", "bad", 0, 0, true},
	} {
		t.Run(tc.workers+"/"+tc.part, func(t *testing.T) {
			t.Setenv("S3_UPLOAD_CONCURRENCY", tc.workers)
			t.Setenv("S3_UPLOAD_PART_SIZE_MIB", tc.part)
			got, err := LoadStorage()
			if (err != nil) != tc.fail {
				t.Fatal(err)
			}
			if !tc.fail && (got.UploadConcurrency != tc.wantWorkers || got.UploadPartSizeMiB != tc.wantPart) {
				t.Fatal(got)
			}
		})
	}
}
