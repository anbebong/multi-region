package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type Authenticator interface {
	ClientTLSConfig() (*tls.Config, error)
	ServerTLSConfig() (*tls.Config, error)
}

type MTLSAuthenticator struct {
	caCertPath string
	certPath   string
	keyPath    string
}

func NewMTLSAuthenticator(caCertPath, certPath, keyPath string) (*MTLSAuthenticator, error) {
	return &MTLSAuthenticator{caCertPath: caCertPath, certPath: certPath, keyPath: keyPath}, nil
}

func (a *MTLSAuthenticator) loadCAPool() (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(a.caCertPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to append CA cert from %s", a.caCertPath)
	}
	return pool, nil
}

func (a *MTLSAuthenticator) loadKeyPair() (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(a.certPath, a.keyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load key pair: %w", err)
	}
	return cert, nil
}

func (a *MTLSAuthenticator) ClientTLSConfig() (*tls.Config, error) {
	pool, err := a.loadCAPool()
	if err != nil {
		return nil, err
	}
	cert, err := a.loadKeyPair()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}, nil
}

func (a *MTLSAuthenticator) ServerTLSConfig() (*tls.Config, error) {
	pool, err := a.loadCAPool()
	if err != nil {
		return nil, err
	}
	cert, err := a.loadKeyPair()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, nil
}
