package client

import (
	"context"
	"github.com/arcward/keyquarry/api"
	"google.golang.org/grpc"
	"log/slog"
	"time"
)

const (
	DefaultDialTimeout          = 10 * time.Second
	DefaultDialKeepAliveTime    = 30 * time.Second
	DefaultDialKeepAliveTimeout = 60 * time.Second
)

type Client struct {
	id            string
	serverAddress string
	client        api.KeyQuarryClient
	adminClient   api.AdminClient
	conn          *grpc.ClientConn
	callOpts      []grpc.CallOption
	dialOpts      []grpc.DialOption
	logger        *slog.Logger
}

func (c *Client) requestLogger(ctx context.Context) *slog.Logger {
	return c.logger.With(
		slog.String("remote", c.conn.Target()),
	)
}

func (c *Client) Shutdown(
	ctx context.Context,
	in *api.ShutdownRequest,
	opts ...grpc.CallOption,
) (*api.ShutdownResponse, error) {
	logger := c.requestLogger(ctx)
	logger.Info("shutting down server")
	opts = append(opts, c.callOpts...)
	rv, err := c.adminClient.Shutdown(ctx, in, opts...)
	return rv, err
}

func (c *Client) Prune(
	ctx context.Context,
	in *api.PruneRequest,
	opts ...grpc.CallOption,
) (*api.PruneResponse, error) {
	logger := c.requestLogger(ctx)
	logger.Info("triggering prune")
	opts = append(opts, c.callOpts...)
	rv, err := c.adminClient.Prune(ctx, in, opts...)
	logger.Info(
		"prune response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) Set(
	ctx context.Context,
	in *api.KeyValue,
	opts ...grpc.CallOption,
) (*api.SetResponse, error) {
	logger := c.requestLogger(ctx)
	logger.Info("setting/updating key", slog.String("key", in.Key))
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Set(ctx, in, opts...)
	logger.Debug(
		"set response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) Get(
	ctx context.Context,
	in *api.Key,
	opts ...grpc.CallOption,
) (*api.GetResponse, error) {
	logger := c.requestLogger(ctx)
	logger.Info("getting value", slog.String("key", in.Key))
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Get(ctx, in, opts...)
	logger.Debug(
		"get response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) GetRevision(
	ctx context.Context,
	in *api.GetRevisionRequest,
) (*api.RevisionResponse, error) {
	logger := c.requestLogger(ctx)
	logger.Info(
		"getting key revision",
		slog.String("key", in.Key),
		slog.Int64("version", in.Version),
	)
	opts := append(c.callOpts, grpc.MaxCallRecvMsgSize(1024*1024*1024))
	rv, err := c.client.GetRevision(ctx, in, opts...)
	logger.Debug(
		"get revision response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) Inspect(
	ctx context.Context,
	in *api.InspectRequest,
	opts ...grpc.CallOption,
) (*api.InspectResponse, error) {
	logger := c.requestLogger(ctx)
	logger.Info("getting key info", slog.String("key", in.Key))
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Inspect(ctx, in, opts...)
	logger.Debug(
		"key info response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) GetKeyMetric(
	ctx context.Context,
	in *api.KeyMetricRequest,
	opts ...grpc.CallOption,
) (*api.KeyMetric, error) {
	logger := c.requestLogger(ctx)
	logger.Info("getting key metrics", slog.String("key", in.Key))
	opts = append(opts, c.callOpts...)
	rv, err := c.client.GetKeyMetric(ctx, in, opts...)
	logger.Debug(
		"get metrics response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) Delete(
	ctx context.Context,
	in *api.DeleteRequest,
	opts ...grpc.CallOption,
) (*api.DeleteResponse, error) {
	logger := c.requestLogger(ctx)
	logger.Info("deleting key", slog.String("key", in.Key))
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Delete(ctx, in, opts...)
	logger.Debug(
		"delete response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err

}

func (c *Client) Exists(
	ctx context.Context,
	in *api.Key,
	opts ...grpc.CallOption,
) (*api.ExistsResponse, error) {
	logger := c.requestLogger(ctx)
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Exists(ctx, in, opts...)
	logger.Debug(
		"exists response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) Pop(
	ctx context.Context,
	in *api.PopRequest,
	opts ...grpc.CallOption,
) (*api.GetResponse, error) {
	logger := c.requestLogger(ctx)
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Pop(ctx, in, opts...)
	logger.Debug("response", slog.Any("response", rv), slog.Any("error", err))
	return rv, err
}

func (c *Client) Clear(
	ctx context.Context,
	in *api.ClearRequest,
	opts ...grpc.CallOption,
) (*api.ClearResponse, error) {
	logger := c.requestLogger(ctx)
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Clear(ctx, in, opts...)
	logger.Debug(
		"clear response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) ListKeys(
	ctx context.Context,
	in *api.ListKeysRequest,
	opts ...grpc.CallOption,
) (*api.ListKeysResponse, error) {
	logger := c.requestLogger(ctx)
	opts = append(opts, c.callOpts...)
	rv, err := c.client.ListKeys(ctx, in, opts...)
	logger.Debug(
		"list keys response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) Stats(
	ctx context.Context,
	in *api.EmptyRequest,
	opts ...grpc.CallOption,
) (*api.ServerMetrics, error) {
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Stats(ctx, in, opts...)
	c.logger.Debug(
		"stats response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) ClearHistory(
	ctx context.Context,
	in *api.EmptyRequest,
	opts ...grpc.CallOption,
) (*api.ClearHistoryResponse, error) {
	logger := c.requestLogger(ctx)
	opts = append(opts, c.callOpts...)
	rv, err := c.client.ClearHistory(ctx, in, opts...)
	logger.Debug(
		"clear history response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) Lock(
	ctx context.Context,
	in *api.LockRequest,
	opts ...grpc.CallOption,
) (*api.LockResponse, error) {
	logger := c.requestLogger(ctx)
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Lock(ctx, in, opts...)
	logger.Debug(
		"lock response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) SetReadOnly(
	ctx context.Context,
	in *api.ReadOnlyRequest,
	opts ...grpc.CallOption,
) (*api.ReadOnlyResponse, error) {
	logger := c.requestLogger(ctx)
	opts = append(opts, c.callOpts...)
	switch {
	case in.Enable:
		logger.Info("attempting to set read-only mode")
	default:
		logger.Info("attempting to disable read-only mode")
	}

	rv, err := c.client.SetReadOnly(ctx, in, opts...)
	logger.Debug(
		"read-only response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) Unlock(
	ctx context.Context,
	in *api.UnlockRequest,
	opts ...grpc.CallOption,
) (*api.UnlockResponse, error) {
	logger := c.requestLogger(ctx)
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Unlock(ctx, in, opts...)
	logger.Debug(
		"unlock response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

// New returns a new instance of Client with the given configuration.
func New(
	address string,
	clientID string,
	logger *slog.Logger,
	callOpts []grpc.CallOption,
	dialOpts ...grpc.DialOption,
) *Client {
	if logger == nil {
		logger = slog.Default().With(
			"logger", "client",
			"remote", address,
			"client_id", clientID,
		)
	}
	dialOpts = append(
		dialOpts,
		grpc.WithPerRPCCredentials(NewClientIDCredentials(clientID)),
	)
	client := &Client{
		id:            clientID,
		serverAddress: address,
		logger:        logger,
		callOpts:      callOpts,
		dialOpts:      dialOpts,
	}
	return client
}

func (c *Client) ServerAddress() string {
	return c.serverAddress
}

// ClientIDCredentials implements the credentials.PerRPCCredentials interface,
// adding the client ID to the request metadata. Thi
type ClientIDCredentials struct {
	clientID string
}

func (c *ClientIDCredentials) GetRequestMetadata(
	ctx context.Context,
	uri ...string,
) (map[string]string, error) {
	return map[string]string{"client_id": c.clientID}, nil
}

func (c *ClientIDCredentials) RequireTransportSecurity() bool {
	return false
}

// NewClientIDCredentials returns a new instance of ClientIDCredentials
// with the given client ID.
func NewClientIDCredentials(clientID string) *ClientIDCredentials {
	return &ClientIDCredentials{clientID: clientID}
}

func (c *Client) WatchKeyValue(
	ctx context.Context,
	in *api.WatchKeyValueRequest,
) (api.KeyQuarry_WatchKeyValueClient, error) {
	logger := c.requestLogger(ctx)
	opts := append(c.callOpts, grpc.MaxCallRecvMsgSize(1024*1024*1024))
	stream, err := c.client.WatchKeyValue(ctx, in, opts...)
	if err != nil {
		logger.Error(
			"error creating key value watch stream",
			slog.String("error", err.Error()),
		)
		return nil, err
	}
	return stream, nil
}

func (c *Client) WatchStream(
	ctx context.Context,
	in *api.WatchRequest,
) (api.KeyQuarry_WatchStreamClient, error) {
	logger := c.requestLogger(ctx)
	opts := append(c.callOpts, grpc.MaxCallRecvMsgSize(1024*1024*1024))
	stream, err := c.client.WatchStream(ctx, in, opts...)
	if err != nil {
		logger.Error(
			"error creating watch stream",
			slog.String("error", err.Error()),
		)
		return nil, err
	}
	return stream, nil
}

func (c *Client) Register(
	ctx context.Context,
	in *api.RegisterRequest,
	opts ...grpc.CallOption,
) (*api.RegisterResponse, error) {
	logger := c.requestLogger(ctx)
	opts = append(opts, c.callOpts...)
	rv, err := c.client.Register(ctx, in, opts...)
	logger.Debug(
		"register response",
		slog.Any("response", rv),
		slog.Any("error", err),
	)
	return rv, err
}

func (c *Client) CloseConnection() error {
	if c.conn != nil {
		if e := c.conn.Close(); e != nil {
			c.logger.Error(
				"error closing connection",
				slog.String("error", e.Error()),
			)
			return e
		}
	}
	return nil
}

func (c *Client) Dial(
	ctx context.Context,
	register bool,
) error {
	if c.conn != nil {
		if e := c.conn.Close(); e != nil {
			c.logger.Error(
				"error closing connection",
				slog.String("error", e.Error()),
			)
		}
	}
	c.logger.Info("connecting")

	conn, err := grpc.DialContext(
		ctx,
		c.serverAddress,
		c.dialOpts...,
	)
	if err != nil {
		c.logger.Error(
			"error connecting",
			"error", err,
		)
		return err
	}
	c.logger.Info("Connected")
	c.conn = conn
	c.client = api.NewKeyQuarryClient(conn)
	c.adminClient = api.NewAdminClient(conn)
	if register {
		reg, regErr := c.Register(ctx, &api.RegisterRequest{})
		if regErr != nil {
			return regErr
		}
		c.logger.Info("Registered", slog.String("client_id", reg.ClientId))
	}
	return nil
}
