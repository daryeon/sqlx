package sqlx

import (
	"context"
	"database/sql"
	"math/rand"
	"time"
)

type _Key int

const (
	_KeyDB = _Key(iota + 1)
	_KeyJustWDB
	_KeyTx
)

var wDB *DB
var rDBs []*DB

func OpenWriteableDB(driverName string, dsn string) (*DB, error) {
	v, e := Open(driverName, dsn)
	wDB = v
	return v, e
}

func OpenReadonlyDB(driverName string, dsn string) (*DB, error) {
	v, e := Open(driverName, dsn)
	if e == nil {
		rDBs = append(rDBs, v)
	}
	return v, e
}

var PickReadonlyDB func([]*DB) *DB

func init() {
	rand.Seed(time.Now().UnixNano())
	PickReadonlyDB = func(dbs []*DB) *DB { return dbs[rand.Int()%len(dbs)] }
}

func JustWriteableDB(ctx context.Context) context.Context {
	return context.WithValue(ctx, _KeyJustWDB, true)
}

func PickExecutor(ctx context.Context) (context.Context, Executor) {
	txi := ctx.Value(_KeyTx)
	if txi != nil {
		return ctx, txi.(*Tx)
	}

	dbi := ctx.Value(_KeyDB)
	if dbi != nil {
		return ctx, dbi.(*DB)
	}

	var db *DB
	if ctx.Value(_KeyJustWDB) != nil {
		db = wDB
	} else {
		if len(rDBs) > 0 {
			db = PickReadonlyDB(rDBs)
		} else {
			db = wDB
		}
	}
	return context.WithValue(ctx, _KeyDB, db), db
}

type TxOptions struct {
	sql.TxOptions
	Savepoint string
}

func BeginTx(ctx context.Context, options *TxOptions) (context.Context, *Tx) {
	ctx, exe := PickExecutor(ctx)
	var tx *Tx
	switch tv := exe.(type) {
	case *Tx:
		tx = tv.MustBeginTx(ctx, options.Savepoint)
	default:
		var opt *sql.TxOptions
		if options != nil {
			opt = &options.TxOptions
		}
		ctx = context.WithValue(ctx, _KeyDB, wDB)
		tx = wDB.MustBeginTx(ctx, opt)
	}
	return context.WithValue(ctx, _KeyTx, tx), tx
}

func WithTx(ctx context.Context, tx *Tx) context.Context { return context.WithValue(ctx, _KeyTx, tx) }

func WithDB(ctx context.Context, db *DB) context.Context { return context.WithValue(ctx, _KeyDB, db) }
