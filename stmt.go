package sqlx

import (
	"context"
	"database/sql"
)

type Stmt struct {
	std    *sql.Stmt
	keys   []string
	logger Logger
}

func (stmt *Stmt) Close() error {
	if stmt.logger != nil {
		stmt.logger.Printf("stmt close, sql.Stmt(%p)", stmt.std)
	}
	return stmt.std.Close()
}

func (stmt *Stmt) Execute(ctx context.Context, params interface{}) (sql.Result, error) {
	args, err := ParamsToArgs(params, stmt.keys)
	if err != nil {
		return nil, err
	}
	if stmt.logger != nil {
		stmt.logger.Printf("stmt execute, args(%v), sql.Stmt(%p)", args, stmt.std)
	}
	return stmt.std.ExecContext(ctx, args...)
}

func (stmt *Stmt) Rows(ctx context.Context, params interface{}) (*Rows, error) {
	args, err := ParamsToArgs(params, stmt.keys)
	if err != nil {
		return nil, err
	}
	if stmt.logger != nil {
		stmt.logger.Printf("stmt select, args(%v), sql.Stmt(%p)", args, stmt.std)
	}
	rows, err := stmt.std.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: rows}, nil
}

func (stmt *Stmt) Get(ctx context.Context, params interface{}, dist interface{}) error {
	rows, err := stmt.Rows(ctx, params)
	if err != nil {
		return err
	}
	return rows.get(dist)
}

func (stmt *Stmt) Select(ctx context.Context, params interface{}, dist interface{}) error {
	rows, err := stmt.Rows(ctx, params)
	if err != nil {
		return err
	}
	return rows._select(dist)
}

func (stmt *Stmt) GetDirect(ctx context.Context, params interface{}, dist DirectDists) error {
	return stmt.Get(ctx, params, dist)
}

func (stmt *Stmt) GetJoined(ctx context.Context, params interface{}, dist JoinedDist) error {
	rows, err := stmt.Rows(ctx, params)
	if err != nil {
		return err
	}
	return rows.getJoined(dist)
}

func (stmt *Stmt) SelectJoined(ctx context.Context, params interface{}, ptrOfJoinedDistSlice interface{}) error {
	rows, err := stmt.Rows(ctx, params)
	if err != nil {
		return err
	}
	return rows.selectJoined(ptrOfJoinedDistSlice)
}
