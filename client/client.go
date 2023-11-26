package client

import (
	"context"
	"crypto/tls"
	"github.com/arcward/gokv/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"log"
	"log/slog"
	"time"
)

type Client struct {
	client    api.KeyValueStoreClient
	conn      *grpc.ClientConn
	cfg       ClientConfig
	ctx       context.Context
	callOpts  []grpc.CallOption
	dialOpts  []grpc.DialOption
	logger    *slog.Logger
	tlsConfig *tls.Config
}

func (c *Client) Set(
	ctx context.Context,
	in *api.KeyValue,
	opts ...grpc.CallOption,
) (*api.SetResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.Set(ctx, in, opts...)
}

func (c *Client) Get(
	ctx context.Context,
	in *api.Key,
	opts ...grpc.CallOption,
) (*api.GetResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.Get(ctx, in, opts...)
}

func (c *Client) GetRevision(
	ctx context.Context,
	in *api.GetRevisionRequest,
) (*api.RevisionResponse, error) {
	opts := append(c.callOpts, grpc.MaxCallRecvMsgSize(1024*1024*1024))
	return c.client.GetRevision(ctx, in, opts...)
}

func (c *Client) GetKeyInfo(
	ctx context.Context,
	in *api.Key,
	opts ...grpc.CallOption,
) (*api.GetKeyValueInfoResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.GetKeyInfo(ctx, in, opts...)
}

func (c *Client) Delete(
	ctx context.Context,
	in *api.Key,
	opts ...grpc.CallOption,
) (*api.DeleteResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.Delete(ctx, in, opts...)
}

func (c *Client) Exists(
	ctx context.Context,
	in *api.Key,
	opts ...grpc.CallOption,
) (*api.ExistsResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.Exists(ctx, in, opts...)
}

func (c *Client) Pop(
	ctx context.Context,
	in *api.Key,
	opts ...grpc.CallOption,
) (*api.GetResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.Pop(ctx, in, opts...)
}

func (c *Client) Clear(
	ctx context.Context,
	in *api.EmptyRequest,
	opts ...grpc.CallOption,
) (*api.ClearResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.Clear(ctx, in, opts...)
}

func (c *Client) ListKeys(
	ctx context.Context,
	in *api.ListKeysRequest,
	opts ...grpc.CallOption,
) (*api.ListKeysResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.ListKeys(ctx, in, opts...)
}

func (c *Client) Stats(
	ctx context.Context,
	in *api.EmptyRequest,
	opts ...grpc.CallOption,
) (*api.ServerMetrics, error) {
	opts = append(opts, c.callOpts...)
	return c.client.Stats(ctx, in, opts...)
}

func (c *Client) ClearHistory(
	ctx context.Context,
	in *api.EmptyRequest,
	opts ...grpc.CallOption,
) (*api.ClearHistoryResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.ClearHistory(ctx, in, opts...)
}

func (c *Client) Lock(
	ctx context.Context,
	in *api.LockRequest,
	opts ...grpc.CallOption,
) (*api.LockResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.Lock(ctx, in, opts...)
}

func (c *Client) Unlock(
	ctx context.Context,
	in *api.UnlockRequest,
	opts ...grpc.CallOption,
) (*api.UnlockResponse, error) {
	opts = append(opts, c.callOpts...)
	return c.client.Unlock(ctx, in, opts...)
}

type ClientConfig struct {
	Address string          `json:"address"`
	Context context.Context `json:"-"`
	Logger  *slog.Logger    `json:"-"`

	// DialTimeout is the timeout for failing to establish a connection.
	DialTimeout time.Duration `json:"dial-timeout"`

	// DialKeepAliveTime is the time after which client pings the server to see if
	// transport is alive.
	DialKeepAliveTime time.Duration `json:"dial-keep-alive-time"`

	// DialKeepAliveTimeout is the time that the client waits for a response for the
	// keep-alive probe. If the response is not received in this time, the connection is closed.
	DialKeepAliveTimeout time.Duration `json:"dial-keep-alive-timeout"`

	InsecureSkipVerify bool   `json:"insecure"`
	NoTLS              bool   `json:"no_tls"`
	SSLCertfile        string `json:"ssl_certfile"`
}

func NewClient(cfg ClientConfig, opts ...grpc.DialOption) *Client {
	client := &Client{cfg: cfg}
	if cfg.Context == nil {
		cfg.Context = context.Background()
	}
	client.ctx = cfg.Context

	if cfg.Logger == nil {
		cfg.Logger = slog.Default().WithGroup("gokv_server")
	}
	client.logger = cfg.Logger

	if cfg.DialKeepAliveTime > 0 {
		params := keepalive.ClientParameters{
			Time:    cfg.DialKeepAliveTime,
			Timeout: cfg.DialKeepAliveTimeout,
		}
		opts = append(opts, grpc.WithKeepaliveParams(params))
	}

	if cfg.DialTimeout > 0 {
		client.ctx, _ = context.WithTimeout(client.ctx, cfg.DialTimeout)
	}

	if cfg.SSLCertfile != "" {
		creds, err := credentials.NewClientTLSFromFile(
			cfg.SSLCertfile,
			"",
		)
		if err != nil {
			log.Fatalln(err)
		}
		opts = append(
			opts,
			grpc.WithTransportCredentials(creds),
		)
	}
	if cfg.InsecureSkipVerify {
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		creds := credentials.NewTLS(tlsConfig)
		opts = append(
			opts,
			grpc.WithTransportCredentials(creds),
		)
	}

	if cfg.NoTLS {
		opts = append(
			opts,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}

	client.dialOpts = append(client.dialOpts, opts...)

	//go func() {
	//	for {
	//		select {
	//		case <-client.ctx.Done():
	//			client.logger.Info("closing connection")
	//			if e := client.conn.Close(); e != nil {
	//				client.logger.Error(
	//					"error closing connection",
	//					slog.String("error", e.Error()),
	//				)
	//			}
	//		}
	//	}
	//}()

	return client
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

func (c *Client) Dial() error {
	if c.conn != nil {
		if e := c.conn.Close(); e != nil {
			c.logger.Error(
				"error closing connection",
				slog.String("error", e.Error()),
			)
		}
	}
	conn, err := grpc.DialContext(c.ctx, c.cfg.Address, c.dialOpts...)
	if err != nil {
		c.logger.Error(
			"error connecting",
			slog.String("error", err.Error()),
			slog.String("address", c.cfg.Address),
		)
		return err
	}
	c.conn = conn
	c.client = api.NewKeyValueStoreClient(conn)
	return nil
}
