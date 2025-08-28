package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

var (
	CAFile               = configFile("ca.pem")
	ServerCertFile       = configFile("server.pem")
	ServerKeyFile        = configFile("server-key.pem")
	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NobodyClientCertFile = configFile("nobody-client.pem")
	NobodyClientKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile         = configFile("model.conf")
	ACLPolicyFile        = configFile("policy.csv")
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalln("Failed to get user home directory:", err)
	}
	return filepath.Join(homeDir, ".proglog", filename)
}

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}

	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		ca := x509.NewCertPool()
		if !ca.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("failed to parse root certificate: %q", cfg.CAFile)
		}

		if cfg.Server {
			// Server *tls.Config is setup to verify the client's certificate and allow the client
			// to verify the server's certificate by setting its ClientCAs
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			// Client *tls.Config is setup to verify server's certificate with the client's by
			// setting the *tls.Config's RootCAs
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}

	return tlsConfig, nil
}
