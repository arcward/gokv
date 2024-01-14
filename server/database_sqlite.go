package server

var SQLite = sqlDialect{
	Driver:       "sqlite",
	DriverPrefix: "sqlite://",
	CreateSnapshotTable: `
		CREATE TABLE IF NOT EXISTS snapshots (
		    id INTEGER PRIMARY KEY AUTOINCREMENT,
		    data BLOB,
		    created DATETIME default CURRENT_TIMESTAMP,
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
		WHERE server_name = ? 
		ORDER BY id DESC 
		LIMIT 1;`,
	InsertNewSnapshot: `
		INSERT INTO snapshots (data, server_name) 
		VALUES (?, ?) 
		RETURNING id;
	`,
	GetSnapshotByID: `
		SELECT 
			id, 
			data, 
			created, 
			server_name 
		FROM snapshots 
		WHERE id = ?;
	`,
}
