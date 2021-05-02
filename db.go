package sqlx

import (
	"context"
	"database/sql"
)

type Logger interface {
	Printf(fmt string, args ...interface{})
}

type DB struct {
	std        *sql.DB
	driverType DriverType
	logger     Logger
}

func (db *DB) Raw() *sql.DB { return db.std }

func (db *DB) SetLogger(v Logger) { db.logger = v }

func (db *DB) BindParams(query string, params interface{}) (string, []interface{}, error) {
	q, keys := BindParams(db.driverType, query)
	args, err := ParamsToArgs(params, keys)
	if err != nil {
		return "", nil, err
	}

	if db.logger != nil {
		db.logger.Printf("%s, %v", q, args)
	}
	return q, args, nil
}

func (db *DB) Execute(ctx context.Context, query string, params interface{}) (sql.Result, error) {
	q, a, e := db.BindParams(query, params)
	if e != nil {
		return nil, e
	}
	return db.std.ExecContext(ctx, q, a...)
}

func (db *DB) Rows(ctx context.Context, query string, params interface{}) (*Rows, error) {
	q, a, e := db.BindParams(query, params)
	if e != nil {
		return nil, e
	}
	rows, err := db.std.QueryContext(ctx, q, a...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: rows}, nil
}

func (db *DB) Get(ctx context.Context, query string, params interface{}, dist interface{}) error {
	return get(ctx, db, query, params, dist)
}

func (db *DB) Select(ctx context.Context, query string, params interface{}, slicePtr interface{}) error {
	return _select(ctx, db, query, params, slicePtr)
}

func (db *DB) GetDirect(ctx context.Context, query string, params interface{}, dist DirectDists) error {
	return getDirect(ctx, db, query, params, dist)
}

func (db *DB) GetJoined(ctx context.Context, query string, params interface{}, dist JoinedDist) error {
	return getJoined(ctx, db, query, params, dist)
}

func (db *DB) SelectJoined(ctx context.Context, query string, params interface{}, dist interface{}) error {
	return selectJoined(ctx, db, query, params, dist)
}

func (db *DB) BeginTx(ctx context.Context, opt *sql.TxOptions) (*Tx, error) {
	tx, err := db.std.BeginTx(ctx, opt)
	if err != nil {
		return nil, err
	}
	if db.logger != nil {
		db.logger.Printf("tx begin, sql.Tx(%p);", tx)
	}
	return &Tx{std: tx, db: db, ctx: ctx}, nil
}

func (db *DB) MustBeginTx(ctx context.Context, opt *sql.TxOptions) *Tx {
	t, e := db.BeginTx(ctx, opt)
	if e != nil {
		panic(e)
	}
	return t
}

func (db *DB) Prepare(ctx context.Context, query string) (*Stmt, error) {
	query, keys := BindParams(db.driverType, query)
	stmt, err := db.std.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if db.logger != nil {
		db.logger.Printf("stmt prepared by db: %s, sql.Stmt(%p)", query, stmt)
	}
	return &Stmt{
		std:    stmt,
		keys:   keys,
		logger: db.logger,
	}, nil
}

var _ Executor = (*DB)(nil)

func Open(driverName string, dsn string) (*DB, error) {
	db, e := sql.Open(driverName, dsn)
	if e != nil {
		return nil, e
	}
	return &DB{std: db, driverType: nameToDriverType(driverName)}, nil
}

func MustOpen(driverName string, dsn string) *DB {
	v, e := Open(driverName, dsn)
	if e != nil {
		panic(e)
	}
	return v
}
