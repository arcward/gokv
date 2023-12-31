package client

import (
	"context"
	"crypto/tls"
	"github.com/arcward/keyquarry/api"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"log"
	"log/slog"
	"time"
)

const (
	DefaultDialTimeout          = 5 * time.Second
	DefaultDialKeepAliveTime    = 10 * time.Second
	DefaultDialKeepAliveTimeout = 60 * time.Second
)

type Client struct {
	client   api.KeyValueStoreClient
	conn     *grpc.ClientConn
	cfg      Config
	callOpts []grpc.CallOption
	dialOpts []grpc.DialOption
	logger   *slog.Logger
}

func (c *Client) DialOpts() []grpc.DialOption {
	return c.cfg.DialOpts()
}
func (c *Client) requestLogger(ctx context.Context) *slog.Logger {
	return c.logger.With(
		slog.String("remote", c.conn.Target()),
		slog.String("client_id", c.cfg.ClientID),
	)
}

func (c *Client) Config() Config {
	return c.cfg
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
		slog.Uint64("version", in.Version),
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

func (c *Client) GetKeyInfo(
	ctx context.Context,
	in *api.Key,
	opts ...grpc.CallOption,
) (*api.GetKeyValueInfoResponse, error) {
	logger := c.requestLogger(ctx)
	logger.Info("getting key info", slog.String("key", in.Key))
	opts = append(opts, c.callOpts...)
	rv, err := c.client.GetKeyInfo(ctx, in, opts...)
	logger.Debug(
		"key info response",
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
	if in.Enable {
		logger.Info("attempting to set read-only mode")
	} else {
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

type Config struct {
	Address    string       `json:"address"`
	Logger     *slog.Logger `json:"-"`
	Verbose    bool         `json:"verbose"`
	CACert     string       `json:"ca_certfile"`
	Quiet      bool         `json:"quiet"`
	IndentJSON int          `json:"indent_json"`
	ClientID   string       `mapstructure:"client_id"`

	// DialTimeout is the timeout for failing to establish a connection.
	DialTimeout time.Duration `json:"dial-timeout"`

	// DialKeepAliveTime is the time after which client pings the server to see if
	// transport is alive.
	DialKeepAliveTime time.Duration `json:"dial-keep-alive-time"`

	// DialKeepAliveTimeout is the time that the client waits for a response for the
	// keep-alive probe. If the response is not received in this time, the connection is closed.
	DialKeepAliveTimeout time.Duration `json:"dial-keep-alive-timeout"`

	InsecureSkipVerify bool `json:"insecure"`
	NoTLS              bool `json:"no_tls"`
	ExtraDialOpts      []grpc.DialOption
}

func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("address", c.Address),
		slog.Bool("verbose", c.Verbose),
		slog.String("ca_certfile", c.CACert),
		slog.Bool("quiet", c.Quiet),
		slog.Int("indent_json", c.IndentJSON),
		//slog.String("client_id", c.ClientID),
		slog.Duration("dial_timeout", c.DialTimeout),
		slog.Duration("dial_keep_alive_time", c.DialKeepAliveTime),
		slog.Duration("dial_keep_alive_timeout", c.DialKeepAliveTimeout),
		slog.Bool("insecure", c.InsecureSkipVerify),
		slog.Bool("no_tls", c.NoTLS),
	)
}

func (c Config) DialOpts() []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:    c.DialKeepAliveTime,
				Timeout: c.DialKeepAliveTimeout,
			},
		),
	}

	var tlsEnabled bool

	if c.CACert != "" {
		creds, err := credentials.NewClientTLSFromFile(
			c.CACert,
			"",
		)
		if err != nil {
			log.Fatalln(err)
		}
		opts = append(
			opts,
			grpc.WithTransportCredentials(creds),
		)
		tlsEnabled = true
	}
	if c.InsecureSkipVerify {
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		creds := credentials.NewTLS(tlsConfig)
		opts = append(
			opts,
			grpc.WithTransportCredentials(creds),
		)
		tlsEnabled = true
	}
	if c.NoTLS || !tlsEnabled {
		if c.Logger == nil {
			slog.Warn("TLS is disabled")
		} else {
			c.Logger.Warn("TLS is disabled")
		}
		insc := grpc.WithTransportCredentials(insecure.NewCredentials())
		opts = append(opts, insc)
	}
	opts = append(
		opts,
		grpc.WithUnaryInterceptor(ClientIDInterceptor(c.ClientID)),
	)
	if c.ExtraDialOpts != nil {
		opts = append(opts, c.ExtraDialOpts...)
	}
	return opts
}

func DefaultConfig() Config {
	cfg := Config{
		DialTimeout:          DefaultDialTimeout,
		DialKeepAliveTime:    DefaultDialKeepAliveTime,
		DialKeepAliveTimeout: DefaultDialKeepAliveTimeout,
		NoTLS:                true,
		Logger:               slog.Default().With("logger", "client"),
	}
	return cfg
}

func NewClient(cfg *Config, opts ...grpc.DialOption) *Client {
	defaultConfig := DefaultConfig()
	if cfg == nil {
		cfg = &defaultConfig
	}
	cfg.ExtraDialOpts = opts
	client := &Client{cfg: *cfg}

	if cfg.Logger == nil {
		cfg.Logger = defaultConfig.Logger
	}
	if cfg.ClientID == "" {
		cfg.ClientID = uuid.NewString()
		cfg.Logger.Info(
			"generated client ID",
			slog.String("client_id", cfg.ClientID),
		)
	}

	cfg.Logger = cfg.Logger.With("client_id", cfg.ClientID)
	client.logger = cfg.Logger

	if cfg.DialKeepAliveTime == 0 {
		cfg.DialKeepAliveTime = defaultConfig.DialKeepAliveTime
	}
	if cfg.DialKeepAliveTimeout == 0 {
		cfg.DialKeepAliveTimeout = defaultConfig.DialKeepAliveTimeout
	}

	return client
}

func ClientIDInterceptor(clientID string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "client_id", clientID)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
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
	dialOpts ...grpc.DialOption,
) error {
	if c.conn != nil {
		if e := c.conn.Close(); e != nil {
			c.logger.Error(
				"error closing connection",
				slog.String("error", e.Error()),
			)
		}
	}
	c.logger.Info("connecting", "config", c.cfg)
	opts := c.cfg.DialOpts()
	opts = append(opts, dialOpts...)
	conn, err := grpc.DialContext(
		ctx,
		c.cfg.Address,
		opts...,
	)
	if err != nil {
		c.logger.Error(
			"error connecting",
			"error", err,
			"config", c.cfg,
		)
		return err
	}
	c.logger.Info("Connected", slog.String("address", c.cfg.Address))
	c.conn = conn
	c.client = api.NewKeyValueStoreClient(conn)
	if register {
		reg, regErr := c.Register(ctx, &api.RegisterRequest{})
		if regErr != nil {
			return regErr
		}
		c.logger.Info("Registered", slog.String("client_id", reg.ClientId))
	}
	return nil
}
