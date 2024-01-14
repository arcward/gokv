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

type sqlDialect struct {
	Driver                           string
	DriverPrefix                     string
	CreateSnapshotTable              string
	SelectLatestSnapshotByServerName string
	InsertNewSnapshot                string
	GetSnapshotByID                  string
}

func (s *sqlDialect) InitDB(ctx context.Context, connStr string) error {
	driverName, connStr := DBConnStr(connStr)
	if driverName != s.Driver {
		return fmt.Errorf(
			"expected driver '%s', got '%s'",
			s.Driver,
			driverName,
		)
	}
	db, err := sql.Open(driverName, connStr)
	go func() {
		_ = db.Close()
	}()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, s.CreateSnapshotTable)
	if err != nil {
		return err
	}

	return nil
}

func (s *sqlDialect) AddSnapshot(
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

func (s *sqlDialect) DBConn(connStr string) (*sql.DB, error) {
	driverName, connStr := DBConnStr(connStr)
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

func (s *sqlDialect) GetSnapshot(ctx context.Context, db *sql.DB, id int64) (
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

func (s *sqlDialect) GetLatestSnapshot(
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

func DBConnStr(s string) (driverName string, connStr string) {
	if strings.HasPrefix(s, PSQL.DriverPrefix) {
		return PSQL.Driver, s
	}
	if strings.HasPrefix(s, SQLite.DriverPrefix) {
		connStr, _ = strings.CutPrefix(s, SQLite.DriverPrefix)
		return SQLite.Driver, connStr
	}
	return "", ""
}

func GetDialect(connStr string) *sqlDialect {
	if strings.HasPrefix(connStr, PSQL.DriverPrefix) {
		return &PSQL
	}
	if strings.HasPrefix(connStr, SQLite.DriverPrefix) {
		// connStr, _ = strings.CutPrefix(s, SQLite.DriverPrefix)
		return &SQLite
	}
	return nil
}

func ServerFromDB(
	ctx context.Context,
	cfg *Config,
) (*KeyValueStore, error) {
	dialect := GetDialect(cfg.Snapshot.Database)
	if dialect == nil {
		return nil, fmt.Errorf("unsupported database driver")
	}
	db, err := dialect.DBConn(cfg.Snapshot.Database)
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

type SnapshotRecord struct {
	ID         int64
	Data       []byte
	Created    time.Time
	ServerName string
}
