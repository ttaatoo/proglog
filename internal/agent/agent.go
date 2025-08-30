package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	api "github.com/ttaaoo/proglog/api/v1"
	"github.com/ttaaoo/proglog/internal/auth"
	"github.com/ttaaoo/proglog/internal/discovery"
	"github.com/ttaaoo/proglog/internal/log"
	"github.com/ttaaoo/proglog/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	// ServerTLSConfig defines the configuration of the certificate that's
	// served to clients
	ServerTLSConfig *tls.Config
	// PeerTLSConfig defines the configuration of the certificate that's
	// served between servers so they can connect with and replicate each other
	PeerTLSConfig  *tls.Config
	DataDir        string
	BindAddr       string
	RPCPort        int
	NodeName       string
	StartJoinAddrs []string
	ACLModelFile   string
	ACLPolicyFile  string
}

// An Agent runs on every service instance, setting up and connecting
// all the different components. The struct references each component (
// log, server, membership, and replicator) that the Agent manages.
type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		// a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(
		a.Config.DataDir,
		log.Config{},
	)
	return err
}

func (a *Agent) setupServer() error {
	authorizer, err := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)
	if err != nil {
		return err
	}

	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := a.server.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

// setupMembership sets up a Replicator with the gRPC dial options needed to connect
// to other servers and a client so the replicator can connect to other servers,
// consume their data, and produce a copy of the data to the local server.
// Then we create a Membership passing in the replicator and its handler to notify the replicator
// when servers join and leave the cluster.
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	var opts []grpc.DialOption
	if a.Config.PeerTLSConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(a.Config.PeerTLSConfig)))
	}

	conn, err := grpc.NewClient(rpcAddr, opts...)
	if err != nil {
		return err
	}

	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}

	a.membership, err = discovery.New(a.replicator, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})

	return err
}

// This ensures that the agent will shut down once even if
// people call Shutdown() multiple times.
// Then we shut down the agent and its components by:
//  1. Leaving the membership so that other servers will see that this server has left the cluster,
//     so that this server doesn't receive discovery events anymore;
//  2. Closing the replicator so it doesn't continue to replicate;
//  3. Gracefully stopping the gRPC server;
//  4. Closing the log.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}
