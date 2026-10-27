// Package testhttp executes HTTP handlers in memory for offline unit tests.
package testhttp

import (
	"net/http"
	"net/http/httptest"
)

type transport struct{ handler http.Handler }

func (t transport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	w := httptest.NewRecorder()
	t.handler.ServeHTTP(w, r)
	return w.Result(), nil
}

type Server struct {
	URL     string
	handler http.Handler
}

func NewServer(h http.Handler) *Server { return &Server{URL: "http://qdrant.test", handler: h} }
func (s *Server) Close()               {}
func (s *Server) Client() *http.Client { return &http.Client{Transport: transport{s.handler}} }
