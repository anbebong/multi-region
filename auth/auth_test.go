package auth

import (
	"crypto/tls"
	"testing"
)

func TestMTLSAuthenticator_ConfigsLoad(t *testing.T) {
	caCertPath, certPath, keyPath := GenerateTestCA(t)
	a, err := NewMTLSAuthenticator(caCertPath, certPath, keyPath)
	if err != nil {
		t.Fatalf("NewMTLSAuthenticator: %v", err)
	}

	clientCfg, err := a.ClientTLSConfig()
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	if len(clientCfg.Certificates) != 1 {
		t.Fatalf("expected 1 client certificate, got %d", len(clientCfg.Certificates))
	}

	serverCfg, err := a.ServerTLSConfig()
	if err != nil {
		t.Fatalf("ServerTLSConfig: %v", err)
	}
	if serverCfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected ClientAuth to be RequireAndVerifyClientCert, got %v", serverCfg.ClientAuth)
	}
}
