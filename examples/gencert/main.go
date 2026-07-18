// Command gencert generates a throwaway CA and one leaf certificate per
// node name given on the command line, writing them as PEM files. It
// exists purely so you can try out mTLS + WithAuthorizeChild /
// allowed_child_ids by hand, without needing openssl/cfssl/Vault installed.
//
// Each leaf certificate's CommonName is set to the node name it was
// generated for (e.g. "root", "branch-1") — this is what
// examples/node's authorizeChildByAllowList checks against a connecting
// child's claimed node-id, and what mTLS itself verifies the certificate
// belongs to.
//
// DO NOT use the output of this tool in production — it is a local,
// short-lived (1 year) self-signed CA for manual trials only, exactly like
// auth.GenerateTestCA (which this reuses the same approach as, but as a
// standalone binary instead of a testing.T-scoped helper).
//
//	go run ./examples/gencert -out examples/node/certs root branch-1 leaf-1
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func main() {
	outDir := flag.String("out", "certs", "directory to write ca.pem/<name>.pem/<name>.key into")
	flag.Parse()
	names := flag.Args()
	if len(names) == 0 {
		fmt.Fprintln(os.Stderr, "usage: gencert -out <dir> <node-name> [<node-name>...]")
		os.Exit(1)
	}

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "multi-region-example-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		log.Fatalf("create CA cert: %v", err)
	}
	caPath := filepath.Join(*outDir, "ca.pem")
	writePEM(caPath, "CERTIFICATE", caDER)
	log.Printf("wrote %s", caPath)

	for i, name := range names {
		leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			log.Fatalf("generate leaf key for %q: %v", name, err)
		}
		leafTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i) + 2),
			// CommonName == node name: this is the identity mTLS proves and
			// what a service's own AuthorizeChild policy (e.g.
			// examples/node's authorizeChildByAllowList) checks the
			// claimed node-id against.
			Subject:     pkix.Name{CommonName: name},
			DNSNames:    []string{"localhost", name},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
			NotBefore:   time.Now(),
			NotAfter:    time.Now().Add(365 * 24 * time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
		if err != nil {
			log.Fatalf("create leaf cert for %q: %v", name, err)
		}

		certPath := filepath.Join(*outDir, name+".pem")
		writePEM(certPath, "CERTIFICATE", leafDER)
		log.Printf("wrote %s", certPath)

		keyPath := filepath.Join(*outDir, name+".key")
		keyDER := x509.MarshalPKCS1PrivateKey(leafKey)
		writePEM(keyPath, "RSA PRIVATE KEY", keyDER)
		log.Printf("wrote %s", keyPath)
	}
}

func writePEM(path, blockType string, der []byte) {
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		log.Fatalf("encode %s: %v", path, err)
	}
}
