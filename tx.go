package sqlx

import (
	"context"
	"database/sql"
	"fmt"
)

type Tx struct {
	std       *sql.Tx
	db        *DB
	savepoint string
	ctx       context.Context
}

func (tx *Tx) Raw() *sql.Tx { return tx.std }

func (tx *Tx) Database() *DB { return tx.db }

func (tx *Tx) BindParams(query string, params interface{}) (string, []interface{}, error) {
	return tx.db.BindParams(query, params)
}

func (tx *Tx) Execute(ctx context.Context, query string, params interface{}) (sql.Result, error) {
	q, a, e := tx.BindParams(query, params)
	if e != nil {
		return nil, e
	}
	return tx.std.ExecContext(ctx, q, a...)
}

func (tx *Tx) Rows(ctx context.Context, query string, params interface{}) (*Rows, error) {
	q, a, e := tx.BindParams(query, params)
	if e != nil {
		return nil, e
	}
	rows, err := tx.std.QueryContext(ctx, q, a...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: rows}, nil
}

func (tx *Tx) Get(ctx context.Context, query string, params interface{}, dist interface{}) error {
	return get(ctx, tx, query, params, dist)
}

func (tx *Tx) Select(ctx context.Context, query string, params interface{}, slicePtr interface{}) error {
	return _select(ctx, tx, query, params, slicePtr)
}

func (tx *Tx) GetDirect(ctx context.Context, query string, params interface{}, dist DirectDists) error {
	return getDirect(ctx, tx, query, params, dist)
}

func (tx *Tx) GetJoined(ctx context.Context, query string, params interface{}, dist JoinedDist) error {
	return getJoined(ctx, tx, query, params, dist)
}

func (tx *Tx) SelectJoined(ctx context.Context, query string, params interface{}, dist interface{}) error {
	return selectJoined(ctx, tx, query, params, dist)
}

func (tx *Tx) Prepare(ctx context.Context, query string) (*Stmt, error) {
	query, keys := BindParams(tx.db.driverType, query)
	stmt, err := tx.std.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if tx.db.logger != nil {
		tx.db.logger.Printf("stmt prepared by tx: %s, sql.Stmt(%p), sql.Tx(%p)", query, stmt, tx.std)
	}
	return &Stmt{
		std:    stmt,
		keys:   keys,
		logger: tx.db.logger,
	}, nil
}

func (tx *Tx) Stmt(stmt *Stmt) *Stmt {
	v := tx.std.Stmt(stmt.std)
	if tx.db.logger != nil {
		tx.db.logger.Printf("tx wrap stmt: (%p)=>(%p), sql.Tx(%p)", stmt, v, tx.std)
	}
	return &Stmt{std: v, keys: stmt.keys, logger: stmt.logger}
}

var _ Executor = (*Tx)(nil)

func (tx *Tx) BeginTx(ctx context.Context, savepoint string) (*Tx, error) {
	if tx.db.logger != nil {
		tx.db.logger.Printf("tx begin via savepoint, `%s`, sql.Tx(%p);", savepoint, tx.std)
	}

	_, err := tx.Execute(ctx, fmt.Sprintf("SAVEPOINT %s_BEGIN", savepoint), nil)
	if err != nil {
		return nil, err
	}
	return &Tx{std: tx.std, db: tx.db, savepoint: savepoint, ctx: ctx}, nil
}

func (tx *Tx) MustBeginTx(ctx context.Context, savepoint string) *Tx {
	t, e := tx.BeginTx(ctx, savepoint)
	if e != nil {
		panic(e)
	}
	return t
}

func (tx *Tx) Commit() error {
	if len(tx.savepoint) < 1 {
		if tx.db.logger != nil {
			tx.db.logger.Printf("tx commit, sql.Tx(%p);", tx.std)
		}
		return tx.std.Commit()
	}
	if tx.db.logger != nil {
		tx.db.logger.Printf("tx commit via savepoint, `%s`, sql.Tx(%p);", tx.savepoint, tx.std)
	}
	_, err := tx.Execute(tx.ctx, fmt.Sprintf("SAVEPOINT %s", tx.savepoint), nil)
	if err != nil {
		return err
	}
	_, err = tx.Execute(tx.ctx, fmt.Sprintf("RELASE SAVEPOINT %s_BEGIN", tx.savepoint), nil)
	return err
}

func (tx *Tx) Rollback() error {
	if len(tx.savepoint) < 1 {
		if tx.db.logger != nil {
			tx.db.logger.Printf("tx rollback, sql.Tx(%p);", tx.std)
		}
		return tx.std.Rollback()
	}
	if tx.db.logger != nil {
		tx.db.logger.Printf("tx rollback via savepoint, `%s`, sql.Tx(%p);", tx.savepoint, tx.std)
	}
	_, err := tx.Execute(tx.ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s_BEGIN", tx.savepoint), nil)
	return err
}

func (tx *Tx) RollbackTo(savepoint string) error {
	_, err := tx.Execute(tx.ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepoint), nil)
	return err
}

type AutoCommitError struct {
	Recoverd interface{}
	SqlError error
}

func (ace *AutoCommitError) Error() string {
	return fmt.Sprintf("sqlx: auto commit error, recovers(%v), sql error(%v)", ace.Recoverd, ace.SqlError)
}

func (tx *Tx) AutoCommit() {
	v := recover()
	var e error
	if v == nil {
		e = tx.Commit()
		if e == nil {
			return
		}
	} else {
		e = tx.Rollback()
	}
	panic(&AutoCommitError{Recoverd: v, SqlError: e})
}
