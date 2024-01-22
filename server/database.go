package server

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
	"strings"
	"time"
)

// SQLDialect specifies some engine-specific SQL statements
// for snapshots
type SQLDialect struct {
	// Driver is the name of the database driver (sqlite, postgres, ...)
	Driver string
	// DriverPrefix is the string in front of the database connection string
	// which identifies the driver (sqlite://, postgres://, ...)
	DriverPrefix string
	// CreateSnapshotTable is the SQL/DDL to create the snapshot table
	CreateSnapshotTable string
	// SelectLatestSnapshotByServerName is the SQL to select the latest
	// snapshot for a given server name
	SelectLatestSnapshotByServerName string
	// InsertNewSnapshot is the SQL to insert a new snapshot
	InsertNewSnapshot string
	// GetSnapshotByID is the SQL to get a snapshot by its ID
	GetSnapshotByID string
}

// InitDB c
func (s *SQLDialect) InitDB(ctx context.Context, connStr string) error {
	driverName, connStr := dbConnectionStr(connStr)
	if driverName != s.Driver {
		return fmt.Errorf(
			"expected driver '%s', got '%s'",
			s.Driver,
			driverName,
		)
	}
	db, err := sql.Open(driverName, connStr)
	defer func() {
		_ = db.Close()
	}()
	if err != nil {
		return err
	}
	tx, txErr := db.Begin()
	if txErr != nil {
		return txErr
	}
	_, err = db.ExecContext(ctx, s.CreateSnapshotTable)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}

func (s *SQLDialect) addSnapshot(
	ctx context.Context,
	db *sql.DB,
	serverName string,
	data []byte,
) (
	rowID int64,
	err error,
) {
	err = db.QueryRowContext(
		ctx,
		s.InsertNewSnapshot,
		data,
		serverName,
	).Scan(&rowID)
	return rowID, err
}

func (s *SQLDialect) DB(connStr string) (*sql.DB, error) {
	driverName, connStr := dbConnectionStr(connStr)
	if driverName != s.Driver {
		return nil, fmt.Errorf(
			"expected driver '%s', got '%s'",
			s.Driver,
			driverName,
		)
	}
	db, err := sql.Open(driverName, connStr)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (s *SQLDialect) GetSnapshot(ctx context.Context, db *sql.DB, id int64) (
	*SnapshotRecord,
	error,
) {
	var err error
	var record SnapshotRecord
	err = db.QueryRowContext(
		ctx,
		s.GetSnapshotByID,
		id,
	).Scan(&record.ID, &record.Data, &record.Created, &record.ServerName)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *SQLDialect) GetLatestSnapshot(
	ctx context.Context,
	db *sql.DB,
	serverName string,
) (
	*SnapshotRecord,
	error,
) {
	var err error
	var record SnapshotRecord
	err = db.QueryRowContext(
		ctx,
		s.SelectLatestSnapshotByServerName,
		serverName,
	).Scan(&record.ID, &record.Data, &record.Created, &record.ServerName)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func dbConnectionStr(s string) (driverName string, connStr string) {
	if strings.HasPrefix(s, PSQL.DriverPrefix) {
		return PSQL.Driver, s
	}
	if strings.HasPrefix(s, SQLite.DriverPrefix) {
		connStr, _ = strings.CutPrefix(s, SQLite.DriverPrefix)
		return SQLite.Driver, connStr
	}
	return "", ""
}

func GetDialect(connStr string) *SQLDialect {
	switch {
	case strings.HasPrefix(connStr, PSQL.DriverPrefix):
		return &PSQL
	case strings.HasPrefix(connStr, SQLite.DriverPrefix):
		return &SQLite
	default:
		return nil
	}
}

func serverFromDB(
	ctx context.Context,
	cfg *Config,
) (*Server, error) {
	dialect := GetDialect(cfg.Snapshot.Database)
	if dialect == nil {
		return nil, fmt.Errorf("unsupported database driver")
	}
	db, err := dialect.DB(cfg.Snapshot.Database)
	if err != nil {
		return nil, err
	}
	snapshotRecord, err := dialect.GetLatestSnapshot(ctx, db, cfg.Name)

	if err != nil {
		return nil, fmt.Errorf("error getting latest snapshot: %w", err)
	}
	srv, err := NewServerFromSnapshot(snapshotRecord.Data, cfg)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

// SnapshotRecord represents a database record holding a snapshot
type SnapshotRecord struct {
	// ID is the row ID of the snapshot
	ID int64
	// Data is a JSON-encoded Server
	Data []byte
	// Created is the time the snapshot was created
	Created time.Time
	// ServerName is the name of the server that created the snapshot
	ServerName string
}
