package sqlx

import (
	"context"
	"database/sql"
)

type BasicExecutor interface {
	BindParams(query string, params interface{}) (string, []interface{}, error)
	Execute(ctx context.Context, query string, params interface{}) (sql.Result, error)
	Rows(ctx context.Context, query string, params interface{}) (*Rows, error)
	Prepare(ctx context.Context, query string) (*Stmt, error)
}

type Executor interface {
	BasicExecutor
	// Get fetch one row, and scan to dist
	Get(ctx context.Context, query string, params interface{}, dist interface{}) error
	// Select fetch many rows, and scan to dist. dist must be a slice pointer
	Select(ctx context.Context, query string, params interface{}, ptrOfDistSlice interface{}) error
	// GetDirect fetch some specific columns of one row
	GetDirect(ctx context.Context, query string, params interface{}, dist DirectDists) error
	// GetJoined auto scan joined select
	GetJoined(ctx context.Context, query string, params interface{}, dist interface{}, get JoinedGet) error
	// SelectJoined auto scan joined select
	SelectJoined(ctx context.Context, query string, params interface{}, ptrOfJoinedDistSlice interface{}, get JoinedGet) error
}

func get(ctx context.Context, be BasicExecutor, query string, params interface{}, dist interface{}) error {
	rows, err := be.Rows(ctx, query, params)
	if err != nil {
		return err
	}
	defer rows.Close()
	return rows.get(dist)
}

func _select(ctx context.Context, be BasicExecutor, query string, params interface{}, slicePtr interface{}) error {
	rows, err := be.Rows(ctx, query, params)
	if err != nil {
		return err
	}
	defer rows.Close()
	return rows._select(slicePtr)
}

func getDirect(ctx context.Context, be BasicExecutor, query string, params interface{}, dist DirectDists) error {
	rows, err := be.Rows(ctx, query, params)
	if err != nil {
		return err
	}
	defer rows.Close()
	return rows.get(dist)
}

func getJoined(ctx context.Context, be BasicExecutor, query string, params interface{}, dist interface{}, get JoinedGet) error {
	rows, err := be.Rows(ctx, query, params)
	if err != nil {
		return err
	}
	defer rows.Close()
	return rows.getJoined(dist, get)
}

func selectJoined(ctx context.Context, be BasicExecutor, query string, params interface{}, slicePtr interface{}, joinedGet JoinedGet) error {
	rows, err := be.Rows(ctx, query, params)
	if err != nil {
		return err
	}
	defer rows.Close()
	return rows.selectJoined(slicePtr, joinedGet)
}
