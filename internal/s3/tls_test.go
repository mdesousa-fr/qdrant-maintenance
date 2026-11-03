package s3

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func TestInsecureTransportDoesNotChangeDefaultTLS(t *testing.T) {
	original := http.DefaultTransport
	base := original.(*http.Transport).Clone()
	base.TLSClientConfig = &tls.Config{ServerName: "storage.example", MinVersion: tls.VersionTLS12}
	http.DefaultTransport = base
	t.Cleanup(func() { http.DefaultTransport = original })

	transport := insecureTransport()
	if !transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("S3 transport still verifies certificates")
	}
	if base.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("default transport TLS verification was disabled")
	}
	if transport.TLSClientConfig.ServerName != "storage.example" || transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("existing TLS settings were lost")
	}
	transport.TLSClientConfig.ServerName = "changed.example"
	if base.TLSClientConfig.ServerName != "storage.example" {
		t.Fatal("S3 and default transports share TLS settings")
	}
}

func TestInsecureTransportWithoutExistingTLSConfig(t *testing.T) {
	original := http.DefaultTransport
	base := original.(*http.Transport).Clone()
	base.TLSClientConfig = nil
	http.DefaultTransport = base
	t.Cleanup(func() { http.DefaultTransport = original })
	if !insecureTransport().TLSClientConfig.InsecureSkipVerify {
		t.Fatal("S3 transport still verifies certificates")
	}
	if base.TLSClientConfig != nil && base.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("default transport TLS verification was disabled")
	}
}
