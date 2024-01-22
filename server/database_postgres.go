package server

var PSQL = SQLDialect{
	Driver:       "postgres",
	DriverPrefix: "postgres://",
	CreateSnapshotTable: `
		CREATE TABLE IF NOT EXISTS snapshots (
		    id SERIAL PRIMARY KEY,
		    data JSONB,
		    created TIMESTAMP default CURRENT_TIMESTAMP,
		    server_name text
    	);
		CREATE INDEX IF NOT EXISTS snapshots_server_name_idx ON snapshots(server_name);
    `,
	SelectLatestSnapshotByServerName: `
		SELECT 
			id, 
			data, 
			created, 
			server_name 
		FROM snapshots 
		WHERE server_name = $1 
		ORDER BY id DESC 
		LIMIT 1;
	`,
	InsertNewSnapshot: `
		INSERT INTO snapshots (data, server_name) VALUES ($1, $2) RETURNING id;
	`,
	GetSnapshotByID: `
		SELECT 
			id, 
			data, 
			created, 
			server_name 
		FROM snapshots 
		WHERE id = $1;
	`,
}
