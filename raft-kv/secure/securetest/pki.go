// Package securetest generates throwaway certificate authorities and
// certificates for tests. Never use its output outside tests.
package securetest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// PKI holds PEM file paths: a cluster CA, a node certificate valid as both
// TLS server and client for 127.0.0.1/localhost, a client-only
// certificate, and a "rogue" client certificate from an unrelated CA.
type PKI struct {
	CA                    string
	NodeCert, NodeKey     string
	ClientCert, ClientKey string
	RogueCert, RogueKey   string
}

var serial atomic.Int64

func writePEM(path, typ string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600)
}

func newCA(name string) (*x509.Certificate, *ecdsa.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial.Add(1)), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, err
	}
	c, err := x509.ParseCertificate(der)
	return c, key, der, err
}

func leaf(dir, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, usage ...x509.ExtKeyUsage) (string, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial.Add(1)), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		ExtKeyUsage: usage, KeyUsage: x509.KeyUsageDigitalSignature,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return "", "", err
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", "", err
	}
	cp, kp := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	if err := writePEM(cp, "CERTIFICATE", der); err != nil {
		return "", "", err
	}
	return cp, kp, writePEM(kp, "EC PRIVATE KEY", kder)
}

// Generate writes a fresh PKI into dir.
func Generate(dir string) (PKI, error) {
	var p PKI
	ca, caKey, caDER, err := newCA("raft-kv-test-ca")
	if err != nil {
		return p, err
	}
	p.CA = filepath.Join(dir, "ca.crt")
	if err := writePEM(p.CA, "CERTIFICATE", caDER); err != nil {
		return p, err
	}
	if p.NodeCert, p.NodeKey, err = leaf(dir, "node", ca, caKey, x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth); err != nil {
		return p, err
	}
	if p.ClientCert, p.ClientKey, err = leaf(dir, "client", ca, caKey, x509.ExtKeyUsageClientAuth); err != nil {
		return p, err
	}
	rca, rkey, _, err := newCA("rogue-ca")
	if err != nil {
		return p, err
	}
	p.RogueCert, p.RogueKey, err = leaf(dir, "rogue", rca, rkey, x509.ExtKeyUsageClientAuth)
	return p, err
}
