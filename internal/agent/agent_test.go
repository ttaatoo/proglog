package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github.com/ttaaoo/proglog/api/v1"
	"github.com/ttaaoo/proglog/internal/agent"
	"github.com/ttaaoo/proglog/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	// sets up a three-node cluster
	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		// ports[0] is for Serf service discovery connections
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		// ports[1] is for gRPC log connections
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			// the second and third nodes join the first node's cluster
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)
		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()
	time.Sleep(3 * time.Second)

	// checks that we can produce and consume a message from a single node
	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		})
	require.NoError(t, err)

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		})
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("hello world"))

	// now we need to check that another node replicated the record
	// wait until replication has finished
	time.Sleep(3 * time.Second)
	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		})
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("hello world"))
}

func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.NewClient(rpcAddr, opts...)
	require.NoError(t, err)
	client := api.NewLogClient(conn)
	return client
}
