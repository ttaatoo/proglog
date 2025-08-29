package server

import (
	"context"
	"io"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	api "github.com/ttaaoo/proglog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(offset uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

// The constants match the values in the ACL policy file
const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// Consume implements log_v1.LogServer.
func (g *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := g.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := g.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ConsumeStream implements log_v1.LogServer.
func (g *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream grpc.ServerStreamingServer[api.ConsumeResponse]) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := g.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				// if the server has read to the end of the log and there is no more data,
				// just wait until someone produces another record to the client
				continue
			default:
				return err
			}
			if err := stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// Produce implements log_v1.LogServer.
func (g *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := g.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}
	offset, err := g.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// ProduceStream implements log_v1.LogServer.
func (g *grpcServer) ProduceStream(stream grpc.BidiStreamingServer[api.ProduceRequest, api.ProduceResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		res, err := g.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}

	return srv, nil
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts,
		grpc.ChainStreamInterceptor(
			grpc_auth.StreamServerInterceptor(authenticate),
		),
		grpc.ChainUnaryInterceptor(
			grpc_auth.UnaryServerInterceptor(authenticate),
		),
	)
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type subjectContextKey struct{}

// return the client's cert's subject so we can indetify a client and check their access.
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

// this is an interceptor that reads the subject out of the client's cert
// and writes it to the RPC's context.
// With interceptors, you can intercept and modify the execution of each RPC call,
// allowing you to break the request handling into smaller, reusable chunks.
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	// extract the subject from the client's cert
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}
