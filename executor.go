package sqlx

import (
	"context"
	"database/sql"
	"reflect"
)

type BasicExecutor interface {
	BindParams(query string, params interface{}) (string, []interface{}, error)
	Execute(ctx context.Context, query string, params interface{}) (sql.Result, error)
	Rows(ctx context.Context, query string, params interface{}) (*Rows, error)
}

type Executor interface {
	BasicExecutor
	// Get fetch one row, and scan to dist
	Get(ctx context.Context, query string, params interface{}, dist interface{}) error
	// Select fetch many rows, and scan to dist. dist must be a slice pointer
	Select(ctx context.Context, query string, params interface{}, dist interface{}) error
	// GetDirect fetch some specific columns of one row
	GetDirect(ctx context.Context, query string, params interface{}, dist DirectDists) error
}

func get(ctx context.Context, be BasicExecutor, query string, params interface{}, dist interface{}) error {
	rows, err := be.Rows(ctx, query, params)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(dist)
		if err != nil {
			return err
		}
		return nil
	}
	return rows.Err()
}

func _select(ctx context.Context, be BasicExecutor, query string, params interface{}, slicePtr interface{}) error {
	rows, err := be.Rows(ctx, query, params)
	if err != nil {
		return err
	}
	defer rows.Close()

	sliceV := reflect.ValueOf(slicePtr).Elem()
	eleT := sliceV.Type().Elem()
	var vPtr *reflect.Value
	if eleT.Kind() == reflect.Ptr {
		vPtr, err = rows.selectToPointerSlice(sliceV, eleT)
	} else {
		vPtr, err = rows.selectToValueSlice(sliceV, eleT)
	}
	if err != nil {
		return err
	}
	sliceV.Set(*vPtr)
	return nil
}

func getDirect(ctx context.Context, be BasicExecutor, query string, params interface{}, dist DirectDists) error {
	rows, err := be.Rows(ctx, query, params)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(dist)
		if err != nil {
			return err
		}
		return nil
	}
	return rows.Err()
}
