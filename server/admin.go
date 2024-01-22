package server

import (
	"context"
	pb "github.com/arcward/keyquarry/api"
	"google.golang.org/grpc/codes"
	"log/slog"
)

var ErrAdminOnly = KQError{
	Message: "missing or invalid client ID for admin function",
	Code:    codes.PermissionDenied,
}

type Admin struct {
	privilegedClientID string
	srv                *Server
	logger             *slog.Logger
	pb.UnimplementedAdminServer
}

func NewAdminServer(srv *Server) *Admin {
	adminServer := &Admin{
		srv:                srv,
		privilegedClientID: srv.cfg.PrivilegedClientID,
	}
	adminServer.logger = srv.cfg.Logger.With(loggerKey, "admin_server")
	return adminServer
}

func (a *Admin) validatePrivilegedClientID(ctx context.Context) (bool, error) {
	clientID, err := a.srv.ClientIDFromContext(ctx)
	if err != nil {
		return false, ErrAdminOnly
	}
	if clientID != a.privilegedClientID {
		return false, ErrAdminOnly
	}
	return true, nil
}

func (a *Admin) Shutdown(
	ctx context.Context,
	_ *pb.ShutdownRequest,
) (*pb.ShutdownResponse, error) {
	ok, err := a.validatePrivilegedClientID(ctx)
	if !ok || err != nil {
		return nil, err
	}
	a.logger.Log(ctx, LevelNotice, "shutdown requested")
	a.srv.shutdown <- struct{}{}
	return &pb.ShutdownResponse{}, nil
}

func (a *Admin) Prune(
	ctx context.Context,
	req *pb.PruneRequest,
) (*pb.PruneResponse, error) {
	ok, err := a.validatePrivilegedClientID(ctx)
	if !ok || err != nil {
		return nil, err
	}
	a.logger.Log(ctx, LevelNotice, "prune requested")
	rv := a.srv.pruner.Prune(ctx, req.PruneTo, req.IgnoreKeys...)
	return &pb.PruneResponse{Pruned: uint64(len(rv))}, nil
}
