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
		v.readonly = true
		rDBs = append(rDBs, v)
	}
	return v, e
}

var PickReadonlyDB func([]*DB) *DB

func getRDB() *DB {
	if len(rDBs) > 0 {
		return PickReadonlyDB(rDBs)
	}
	return wDB
}

func init() {
	rand.Seed(time.Now().UnixNano())
	PickReadonlyDB = func(dbs []*DB) *DB { return dbs[rand.Int()%len(dbs)] }
}

func JustWriteableDB(ctx context.Context) context.Context {
	return context.WithValue(ctx, _KeyJustWDB, true)
}

func getExe(ctx context.Context) Executor {
	txi := ctx.Value(_KeyTx)
	if txi != nil {
		return txi.(*Tx)
	}
	dbi := ctx.Value(_KeyDB)
	if dbi != nil {
		return dbi.(*DB)
	}
	return nil
}

func PickExecutor(ctx context.Context) (context.Context, Executor) {
	exe := getExe(ctx)
	if exe != nil {
		return ctx, exe
	}

	var db *DB
	if ctx.Value(_KeyJustWDB) != nil {
		db = wDB
	} else {
		db = getRDB()
	}
	return context.WithValue(ctx, _KeyDB, db), db
}

type TxOptions struct {
	sql.TxOptions
	Savepoint      string
	JustWritableDB bool
}

func MustBegin(ctx context.Context, options *TxOptions) (context.Context, *Tx) {
	exe := getExe(ctx)
	var rTx *Tx
	if exe != nil { // options can not be nil
		tx, ok := exe.(*Tx)
		if ok {
			rTx = tx.MustBeginTx(ctx, options.Savepoint)
		}
	}
	if rTx == nil { // options can be nil
		if options != nil {
			var db = wDB
			if options.ReadOnly && !options.JustWritableDB {
				db = getRDB()
			}
			rTx = db.MustBeginTx(ctx, &options.TxOptions)
		} else {
			rTx = wDB.MustBeginTx(ctx, nil)
		}
	}
	return context.WithValue(ctx, _KeyTx, rTx), rTx
}

func WithDB(ctx context.Context, db *DB) context.Context { return context.WithValue(ctx, _KeyDB, db) }

func WithTx(ctx context.Context, tx *Tx) context.Context { return context.WithValue(ctx, _KeyTx, tx) }
