// Package config loads command-specific settings from the environment.
package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const defaultKeepLast = 5

type Config struct {
	Qdrant    QdrantConfig
	Storage   StorageConfig
	Retention RetentionConfig
}
type Node struct {
	ID  string
	URL string
}
type QdrantConfig struct {
	URL     string
	APIKey  string
	Nodes   []Node
	Timeout time.Duration
}
type StorageConfig struct {
	Endpoint       string
	Bucket         string
	Region         string
	ForcePathStyle bool
	Timeout        time.Duration
}
type RetentionConfig struct {
	KeepLast int
	Policy   string
}

// ValidSegment accepts collection/snapshot names without allowing path traversal
// or control characters in logs and object keys.
func ValidSegment(s string) bool {
	if strings.TrimSpace(s) == "" || s == "." || s == ".." {
		return false
	}
	for _, r := range s {
		if r == '/' || r == '\\' || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func endpoint(value string) error {
	u, err := url.Parse(value)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid HTTP(S) endpoint")
	}
	return nil
}
func duration(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return d, nil
}

func LoadQdrant(requireNodes bool) (QdrantConfig, error) {
	c := QdrantConfig{URL: os.Getenv("QDRANT_URL"), APIKey: os.Getenv("QDRANT_API_KEY")}
	var err error
	c.Timeout, err = duration("QDRANT_TIMEOUT", 30*time.Minute)
	if err != nil {
		return c, err
	}
	if value := os.Getenv("QDRANT_NODES"); value != "" && (requireNodes || c.URL == "") {
		var nodes map[string]string
		if err := json.Unmarshal([]byte(value), &nodes); err != nil {
			return c, fmt.Errorf("QDRANT_NODES must be a JSON object of node IDs to direct endpoints: %w", err)
		}
		seen := map[string]bool{}
		for id, addr := range nodes {
			if !ValidSegment(id) {
				return c, fmt.Errorf("invalid node ID %q", id)
			}
			if err := endpoint(addr); err != nil {
				return c, fmt.Errorf("QDRANT_NODES[%s]: %w", id, err)
			}
			addr = strings.TrimRight(addr, "/")
			if seen[addr] {
				return c, fmt.Errorf("duplicate node endpoint for %s", id)
			}
			seen[addr] = true
			c.Nodes = append(c.Nodes, Node{ID: id, URL: addr})
		}
		sort.Slice(c.Nodes, func(i, j int) bool { return c.Nodes[i].ID < c.Nodes[j].ID })
	}
	if requireNodes && len(c.Nodes) == 0 {
		return c, fmt.Errorf("QDRANT_NODES is required for backups; configure every node's direct endpoint")
	}
	if (c.URL == "" || requireNodes) && len(c.Nodes) > 0 {
		c.URL = c.Nodes[0].URL
	}
	if c.URL == "" {
		return c, fmt.Errorf("QDRANT_URL or QDRANT_NODES must be configured")
	}
	if err := endpoint(c.URL); err != nil {
		return c, fmt.Errorf("QDRANT_URL: %w", err)
	}
	return c, nil
}

func LoadStorage() (StorageConfig, error) {
	c := StorageConfig{Endpoint: os.Getenv("S3_ENDPOINT"), Bucket: os.Getenv("S3_BUCKET"), Region: os.Getenv("AWS_REGION")}
	if strings.TrimSpace(c.Bucket) == "" {
		return c, fmt.Errorf("S3_BUCKET must be configured")
	}
	if c.Region == "" {
		c.Region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if c.Endpoint != "" {
		if err := endpoint(c.Endpoint); err != nil {
			return c, fmt.Errorf("S3_ENDPOINT: %w", err)
		}
	}
	var err error
	if value := os.Getenv("S3_FORCE_PATH_STYLE"); value != "" {
		c.ForcePathStyle, err = strconv.ParseBool(value)
		if err != nil {
			return c, fmt.Errorf("S3_FORCE_PATH_STYLE must be a boolean")
		}
	} else {
		c.ForcePathStyle = c.Endpoint != ""
	}
	c.Timeout, err = duration("S3_TIMEOUT", 30*time.Minute)
	return c, err
}
func LoadRetention() (RetentionConfig, error) {
	c := RetentionConfig{KeepLast: defaultKeepLast, Policy: os.Getenv("RETENTION_POLICY")}
	if c.Policy == "" {
		c.Policy = "snapshot"
	}
	if c.Policy != "snapshot" && c.Policy != "run" {
		return c, fmt.Errorf("RETENTION_POLICY must be snapshot or run")
	}
	if value := os.Getenv("RETENTION_KEEP_LAST"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return c, fmt.Errorf("RETENTION_KEEP_LAST must be a nonnegative integer")
		}
		c.KeepLast = n
	}
	return c, nil
}

// Load is retained for callers that need Qdrant and retention settings together.
// Storage-only commands use LoadStorage and never require Qdrant credentials.
func Load() (Config, error) {
	q, err := LoadQdrant(false)
	if err != nil {
		return Config{}, err
	}
	r, err := LoadRetention()
	if err != nil {
		return Config{}, err
	}
	return Config{Qdrant: q, Retention: r}, nil
}
