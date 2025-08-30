package log

import (
	"context"
	"os"
	"sync"

	"github.com/rs/zerolog"
	api "github.com/ttaaoo/proglog/api/v1"
	"google.golang.org/grpc"
)

// Replicator connets to other servers with the gRPC client,
// and we need to configure the client so it can authenticate with the servers.
type Replicator struct {
	DialOptions []grpc.DialOption
	// the replicator connects to other servers with the gRPC client,
	// and we need to configure the client so it can authenticate with the servers.
	LocalServer api.LogClient

	logger *zerolog.Logger

	mu sync.Mutex
	// map of server addresses to a channel, which
	// the replicator uses to stop replicating from a server when the server
	// fails or leaves the cluster.
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

// Join adds the given server address to the list of
// servers to replicate and kicks off the add goroutine to run the actual
// replication logic.
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// already replicating so skip
		return nil
	}

	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])
	return nil
}

// Pull based replication
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)
	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	// The loop consumes the logs from the discovered server in a stream
	// and then produces to the local server to save a copy.
	// We replicate messages from the other server until that server fails or leaves the cluster
	// and the replicator closes the channel for that server, which breaks the loop
	// and ends the replicate() goroutine.
	for {
		select {
		// the replicator closes the channel when Serf receives an event saying
		// that the other server left the cluster, and then this server calls the Leave() method
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err := r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Leave handles the server leaving the cluster by removing the server from the list
// of servers to replicate and closes the server's associated channel.
// Closing the channel signals to the receive in the replicate() goroutine to stop replicating from that server.
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if _, ok := r.servers[name]; !ok {
		// not replicating from this server so skip
		return nil
	}

	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// We use this init() helper to lazily initialize the server map.
// You should use lazy initialization to give your structs a useful zero value
func (r *Replicator) init() {
	if r.logger == nil {
		logger := zerolog.New(os.Stderr).With().Str("service", "replicator").Logger()
		r.logger = &logger
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Close closes the replicator so it doesn't replicate new servers that join
// the cluster and it stops replicating existing servers by causing the replicate()
// goroutines to return.
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	r.closed = true
	close(r.close)
	return nil
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error().Err(err).Str("addr", addr).Msg(msg)
}
