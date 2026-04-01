package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/OneBusAway/vehicle-positions/db"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store manages persistence of vehicle locations to PostgreSQL.
type Store struct {
	pool    *pgxpool.Pool 
	queries *db.Queries //pre-written queries
}

// NewStore connects to PostgreSQL.
func NewStore(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL) //create a postgre connection pool
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	} //handle any error whilst connecting
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}//if we try hitting db and we get an error close db and alert of an error

	return &Store{pool: pool, queries: db.New(pool)}, nil //return a store instance
}

// Migrate runs the database schema migrations.
func (s *Store) Migrate(databaseURL string) error {
	d, err := iofs.New(migrationsFS, "migrations")	 /*migration driver where
	- migrationFS; is a file system in memory containing my sql migration files 
	- migrations: the folder name inside that file system where your migration files are stored
	- iofs: creates a driver to read those files
	*/

	if err != nil {
		return fmt.Errorf("invalid migration source: %w", err)
	} //catching error occuring during driver initialization


	m, err := migrate.NewWithSourceInstance("iofs", d, databaseURL)//creating a migration engine instance
	if err != nil {
		return fmt.Errorf("migration instance error: %w", err)
	}//error handling during m-engine creation

	// Close migration source and database connection when done.
	defer func() {
		srcErr, dbErr := m.Close()
		if srcErr != nil {
			slog.Warn("failed to close migration source", "error", srcErr)
		}//issue with closing migration source
		if dbErr != nil {
			slog.Warn("failed to close migration database connection", "error", dbErr)
		}//issue with closing migration database connection
	}()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to apply migrations: %w", err)
	} //apply pending migrations, catch errors and apply function; only ignore when error is due to no change available.
	return nil
}

// SaveLocation upserts the vehicle and inserts a location point in a single transaction.
func (s *Store) SaveLocation(ctx context.Context, loc *LocationReport) error {

	tx, err := s.pool.Begin(ctx) //start db transaction
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	} //catch transaction starting error,if any 
	defer tx.Rollback(ctx)

	qtx := s.queries.WithTx(tx) //filter all queries associated with this transaction

	
	/*so here we're  receiving information on a vehicle and it's current position
	- we first ensure the existence of vehickle, by updating the vehicle if it already exists or insert a new one if it doesn't
	- then we add a new field for this vehicles position*/
	if err := qtx.UpsertVehicle(ctx, loc.VehicleID); err != nil { 
		return fmt.Errorf("upsert vehicle: %w", err)
	}//update or insert a vehicle, catch errors, if any.

	bearing := pgtype.Float8{}
	if loc.Bearing != nil {
		bearing = pgtype.Float8{Float64: *loc.Bearing, Valid: true}
	}

	speed := pgtype.Float8{}
	if loc.Speed != nil {
		speed = pgtype.Float8{Float64: *loc.Speed, Valid: true}
	}

	accuracy := pgtype.Float8{}
	if loc.Accuracy != nil {
		accuracy = pgtype.Float8{Float64: *loc.Accuracy, Valid: true}
	}

	if err := qtx.InsertLocationPoint(ctx, db.InsertLocationPointParams{
		VehicleID: loc.VehicleID,
		TripID:    loc.TripID,
		Latitude:  loc.Latitude,
		Longitude: loc.Longitude,
		Bearing:   bearing,
		Speed:     speed,
		Accuracy:  accuracy,
		Timestamp: loc.Timestamp,
		DriverID:  loc.DriverID,
	}); err != nil {
		return fmt.Errorf("insert location: %w", err) //error handling
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}//try to finalize this transaction, report errors if any
	return nil
}


// GetRecentLocations retrieves the latest position for each vehicle since the cutoff time.
func (s *Store) GetRecentLocations(ctx context.Context, cutoff time.Time) ([]*LocationReport, error) {
	rows, err := s.queries.GetRecentLocations(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
	if err != nil {
		return nil, fmt.Errorf("query recent locations: %w", err)
	}//get all recent locations from db, we pass in a query and cutoff-time wrapped in a type postgre prefers. 

	locations := make([]*LocationReport, 0, len(rows)) //make an slice of location-reports; currently it's empty but allocate space enough to take all db rows

	for _, row := range rows {
		loc := &LocationReport{
			VehicleID: row.VehicleID,
			TripID:    row.TripID,
			Latitude:  row.Latitude,
			Longitude: row.Longitude,
			Timestamp: row.Timestamp,
			DriverID:  row.DriverID,
		} //create a loc object
		// check if Bearing, Speed and Accuracy are valid before assigning them to loc
		if row.Bearing.Valid {
			v := row.Bearing.Float64
			loc.Bearing = &v
		}
		if row.Speed.Valid {
			v := row.Speed.Float64
			loc.Speed = &v
		}
		if row.Accuracy.Valid {
			v := row.Accuracy.Float64
			loc.Accuracy = &v
		}
		locations = append(locations, loc) //add loc entry to locations slice
	}

	return locations, nil //return locations (i.e current location of all systems)
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close shuts down the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}
