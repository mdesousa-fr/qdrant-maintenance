package qdrant

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const qdrantTimeLayout = "2006-01-02T15:04:05"

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type HTTPError struct {
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("unexpected HTTP status: %d", e.StatusCode)
}

type Client struct {
	URL        string
	APIKey     string
	HTTPClient HTTPClient
}

func NewClient(url string, apiKey string) *Client {
	return &Client{
		URL:    strings.TrimSuffix(url, "/"),
		APIKey: apiKey,
		HTTPClient: &http.Client{
			Timeout:   30 * time.Minute,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
}

type collectionResponse struct {
	Result collectionResult `json:"result"`
}

type collectionResult struct {
	Collections []Collection `json:"collections"`
}

type Collection struct {
	Name string `json:"name"`
}

func (c *Client) do(ctx context.Context, httpMethod string, endpoint string) ([]byte, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		httpMethod,
		fmt.Sprintf("%s/%s", c.URL, strings.TrimPrefix(endpoint, "/")),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	request.Header.Set("api-key", c.APIKey)

	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}

	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, &HTTPError{StatusCode: response.StatusCode}
	}

	return body, nil
}

func (c *Client) GetCollections(ctx context.Context) ([]Collection, error) {
	body, err := c.do(ctx, http.MethodGet, "/collections")
	if err != nil {
		return nil, fmt.Errorf("get collections: %w", err)
	}

	var collectionResponse collectionResponse
	if err := json.Unmarshal(body, &collectionResponse); err != nil {
		return nil, fmt.Errorf("get collections: %w", err)
	}

	return collectionResponse.Result.Collections, nil
}

type getSnapshotResponse struct {
	Result []snapshotJSON `json:"result"`
}

type createSnapshotResponse struct {
	Result snapshotJSON `json:"result"`
}

type snapshotJSON struct {
	Name         string `json:"name"`
	CreationTime string `json:"creation_time"`
	Size         uint64 `json:"size"`
	Checksum     string `json:"checksum"`
}

func (s snapshotJSON) toSnapshot() (Snapshot, error) {
	t, err := time.ParseInLocation(
		qdrantTimeLayout,
		s.CreationTime,
		time.UTC,
	)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s.CreationTime)
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf(
			"parse snapshot creation time %q: %w",
			s.CreationTime,
			err,
		)
	}

	return Snapshot{
		Name:         s.Name,
		CreationTime: t,
		Size:         s.Size,
		Checksum:     s.Checksum,
	}, nil
}

type Snapshot struct {
	Name         string
	CreationTime time.Time
	Size         uint64
	Checksum     string
}

func (c *Client) GetSnapshots(ctx context.Context, collectionName string) ([]Snapshot, error) {
	body, err := c.do(
		ctx,
		http.MethodGet,
		fmt.Sprintf("/collections/%s/snapshots", url.PathEscape(collectionName)),
	)
	if err != nil {
		return nil, fmt.Errorf("get snapshots for collection %q: %w", collectionName, err)
	}

	var getSnapshotResponse getSnapshotResponse
	if err := json.Unmarshal(body, &getSnapshotResponse); err != nil {
		return nil, fmt.Errorf("get snapshots for collection %q: %w", collectionName, err)
	}

	var snapshots []Snapshot
	for _, s := range getSnapshotResponse.Result {
		snapshot, err := s.toSnapshot()
		if err != nil {
			return nil, fmt.Errorf("get snapshots for collection %q: %w", collectionName, err)
		}
		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

func (c *Client) CreateSnapshot(ctx context.Context, collectionName string) (Snapshot, error) {
	body, err := c.do(
		ctx,
		http.MethodPost,
		fmt.Sprintf("/collections/%s/snapshots?wait=true", url.PathEscape(collectionName)),
	)
	if err != nil {
		return Snapshot{}, fmt.Errorf("create snapshot for collection %q: %w", collectionName, err)
	}

	var createSnapshotResponse createSnapshotResponse
	if err := json.Unmarshal(body, &createSnapshotResponse); err != nil {
		return Snapshot{}, fmt.Errorf("create snapshot for collection %q: %w", collectionName, err)
	}

	snapshot, err := createSnapshotResponse.Result.toSnapshot()
	if err != nil {
		return Snapshot{}, fmt.Errorf("create snapshot for collection %q: %w", collectionName, err)
	}

	return snapshot, nil
}

// DownloadSnapshot copies the response directly to dst without buffering the snapshot.
func (c *Client) DownloadSnapshot(ctx context.Context, collection, name string, dst io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/collections/%s/snapshots/%s", c.URL, url.PathEscape(collection), url.PathEscape(name)), nil)
	if err != nil {
		return fmt.Errorf("build download request: %w", err)
	}
	req.Header.Set("api-key", c.APIKey)
	res, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("download snapshot: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return &HTTPError{StatusCode: res.StatusCode}
	}
	_, err = io.Copy(dst, res.Body)
	return err
}
func (c *Client) DeleteSnapshot(ctx context.Context, collection, name string) error {
	_, err := c.do(ctx, http.MethodDelete, fmt.Sprintf("/collections/%s/snapshots/%s?wait=true", url.PathEscape(collection), url.PathEscape(name)))
	return err
}
