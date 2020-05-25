package nap

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/labstack/gommon/log"
	"github.com/tevino/abool"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DB is a logical database with multiple underlying physical databases
// forming a single master multiple slaves topology.
// Reads and writes are automatically directed to the correct physical db.
type DB struct {
	pdbs          []*HealthyDB // Physical databases
	count         uint64       // Monotonically incrementing counter on each query
	overallHealth *abool.AtomicBool
}

type HealthyDB struct {
	Name    string
	DB      *sql.DB
	healthy *abool.AtomicBool
}

func (hDB HealthyDB) IsHealthy() bool {
	return hDB.healthy.IsSet()
}

func (hDB HealthyDB) SetUnhealthy() {
	hDB.healthy.SetTo(false)
}

func (hDB HealthyDB) SetHealthy() {
	hDB.healthy.SetToIf(false, true)
}

// Open concurrently opens each underlying physical db.
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the master and the rest as slaves.
func Open(driverName, dataSourceNames string) (*DB, error) {
	conns := strings.Split(dataSourceNames, ";")
	db := &DB{pdbs: make([]*HealthyDB, len(conns)), overallHealth: abool.NewBool(false)}

	err := scatter(len(db.pdbs), func(i int) (err error) {
		db.pdbs[i] = &HealthyDB{}
		if db.pdbs[i].DB, err = sql.Open(driverName, conns[i]); err == nil {
			db.pdbs[i].healthy = abool.NewBool(true)
		}
		if i == 0 {
			db.pdbs[i].Name = "master"
		} else {
			db.pdbs[i].Name = "slave" + strconv.Itoa(i)
		}
		return err
	})

	if err != nil {
		return nil, err
	}
	db.overallHealth.SetTo(true)

	return db, nil
}

// Close closes all physical databases concurrently, releasing any open resources.
func (db *DB) Close() error {
	return scatter(len(db.pdbs), func(i int) error {
		return db.pdbs[i].DB.Close()
	})
}

// Driver returns the physical database's underlying driver.
func (db *DB) Driver() driver.Driver {
	return db.Master().Driver()
}

// Begin starts a transaction on the master. The isolation level is dependent on the driver.
func (db *DB) Begin() (*sql.Tx, error) {
	return db.Master().Begin()
}

// BeginTx starts a transaction with the provided context on the master.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.Master().BeginTx(ctx, opts)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the master as the underlying physical db.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.Master().Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the master as the underlying physical db.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.Master().ExecContext(ctx, query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (db *DB) Ping() error {
	return scatter(len(db.pdbs), func(i int) error {
		return db.pdbs[i].DB.Ping()
	})
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (db *DB) PingContext(ctx context.Context) error {
	return scatter(len(db.pdbs), func(i int) error {
		return db.pdbs[i].DB.PingContext(ctx)
	})
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (db *DB) Prepare(query string) (Stmt, error) {
	stmts := make([]*sql.Stmt, len(db.pdbs))

	err := scatter(len(db.pdbs), func(i int) (err error) {
		stmts[i], err = db.pdbs[i].DB.Prepare(query)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &stmt{db: db, stmts: stmts}, nil
}

// PrepareContext creates a prepared statement for later queries or executions
// on each physical database, concurrently.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (db *DB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmts := make([]*sql.Stmt, len(db.pdbs))

	err := scatter(len(db.pdbs), func(i int) (err error) {
		stmts[i], err = db.pdbs[i].DB.PrepareContext(ctx, query)
		return err
	})

	if err != nil {
		return nil, err
	}
	return &stmt{db: db, stmts: stmts}, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// Query uses a nextReadAccessDB as the physical db.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.ReadAccessDB().Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// QueryContext uses a nextReadAccessDB as the physical db.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.ReadAccessDB().QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a nextReadAccessDB as the physical db.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.ReadAccessDB().QueryRow(query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRowContext uses a nextReadAccessDB as the physical db.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.ReadAccessDB().QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying physical db.
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (db *DB) SetMaxIdleConns(n int) {
	for i := range db.pdbs {
		db.pdbs[i].DB.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (db *DB) SetMaxOpenConns(n int) {
	for i := range db.pdbs {
		db.pdbs[i].DB.SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	for i := range db.pdbs {
		db.pdbs[i].DB.SetConnMaxLifetime(d)
	}
}

// ReadAccessDB returns one of the physical databases which is a nextReadAccessDB
func (db *DB) ReadAccessDB() *sql.DB {
	slaveIndex := db.nextReadAccessDB(len(db.pdbs))
	hDB := db.pdbs[slaveIndex]
	if hDB.IsHealthy() || !db.overallHealth.IsSet() {
		return hDB.DB
	}
	return db.ReadAccessDB()
}

// Retrieves slice
func (db *DB) Slaves() []*sql.DB {
	if len(db.pdbs) < 2 {
		return nil

	}
	var dbs []*sql.DB
	for i := 1; i < len(db.pdbs); i++ {
		dbs = append(dbs, db.pdbs[i].DB)
	}

	return dbs
}

// Master returns the master physical database
func (db *DB) Master() *sql.DB {
	return db.pdbs[0].DB
}

func (db *DB) nextReadAccessDB(n int) int {
	if n <= 1 {
		return 0
	}
	return int(atomic.AddUint64(&db.count, 1) % uint64(n))
}

func (db *DB) PrintStatus() {
	for i := range db.pdbs {
		log.Info(db.pdbs[i].Name, " is ready: ", db.pdbs[i].IsHealthy())
	}
}

func (db *DB) CheckHealthCtx(ctx context.Context) error {
	wg := sync.WaitGroup{}
	wg.Add(len(db.pdbs))
	allUnhealthy := abool.NewBool(true)
	for i := range db.pdbs {
		go func(hdb *HealthyDB) {
			defer wg.Done()
			if err := hdb.DB.PingContext(ctx); err != nil {
				log.Error(hdb.Name, ":  ", err)
				hdb.SetUnhealthy()
			} else {
				hdb.SetHealthy()
				db.overallHealth.Set()
				allUnhealthy.SetToIf(true, false)
			}
		}(db.pdbs[i])
	}
	wg.Wait()
	if allUnhealthy.IsSet() {
		db.overallHealth.UnSet()
		return errors.New("No mariadb's are available")
	}
	return nil
}

func (db *DB) CheckHealth() error {
	allUnhealthy := true
	for i, healthyDB := range db.pdbs {
		if err := healthyDB.DB.Ping(); err != nil {
			log.Error(err)
			db.pdbs[i].SetUnhealthy()
		} else {
			db.pdbs[i].SetHealthy()
			db.overallHealth.Set()
			allUnhealthy = false
		}
	}
	if allUnhealthy {
		db.overallHealth.UnSet()
		return errors.New("No mariadb's are available")
	}
	return nil
}
