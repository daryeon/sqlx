package sqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type Rows struct {
	*sql.Rows
}

type DirectDists []interface{}

var distsType = reflect.TypeOf(DirectDists{})
var ErrUnexpectedDistType = errors.New("sqlx: unexpected dist type")

func isMapType(t reflect.Type) bool {
	if t == mapType {
		return true
	}
	return t.Key().Kind() == reflect.String && t.Elem().Kind() == reflect.Interface
}

func (rows *Rows) Scan(dist interface{}) error {
	v := reflect.ValueOf(dist).Elem()
	t := v.Type()
	if t.Kind() == reflect.Struct {
		return rows.scanStruct(&v)
	}
	if t == distsType {
		return rows.Rows.Scan(dist.(DirectDists)...)
	}
	if t.Kind() == reflect.Map && isMapType(t) {
		return rows.scanMap(&v)
	}
	return ErrUnexpectedDistType
}

var interfaceType = reflect.TypeOf((*interface{})(nil)).Elem()

func (rows *Rows) scanMap(v *reflect.Value) error {
	var m map[string]interface{}
	if v.Len() < 1 {
		m = map[string]interface{}{}
		v.Set(reflect.ValueOf(m))
	} else {
		m = v.Interface().(map[string]interface{})
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	dists := make([]interface{}, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		dists = append(dists, reflect.New(interfaceType).Interface())
	}

	if err = rows.Rows.Scan(dists...); err != nil {
		return err
	}
	for idx, c := range columns {
		m[c] = reflect.ValueOf(dists[idx]).Elem().Interface()
	}
	return nil
}

func (rows *Rows) scanStruct(v *reflect.Value) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	vm := mapper.FieldMap(*v)
	dists := make([]interface{}, 0, len(columns))
	for _, c := range columns {
		v, ok := vm[c]
		if !ok {
			return fmt.Errorf("sqlx: missing column `%s`", c)
		}
		dists = append(dists, v.Addr().Interface())
	}
	return rows.Rows.Scan(dists...)
}

func (rows *Rows) selectToPointerSlice(sliceV reflect.Value, et reflect.Type) (*reflect.Value, error) {
	et = et.Elem()
	for rows.Next() {
		elePtrV := reflect.New(et)
		if err := rows.Scan(elePtrV.Interface()); err != nil {
			return nil, err
		}
		sliceV = reflect.Append(sliceV, elePtrV)
	}
	return &sliceV, rows.Err()
}

func (rows *Rows) selectToValueSlice(sliceV reflect.Value, et reflect.Type) (*reflect.Value, error) {
	for rows.Next() {
		l := sliceV.Len() + 1
		doAppend := true
		var eleV reflect.Value
		var elePtr interface{}
		if l < sliceV.Cap() {
			doAppend = false
			sliceV.SetLen(l)
			elePtr = eleV.Addr().Interface()
		} else {
			elePtrV := reflect.New(et)
			elePtr = elePtrV.Interface()
			eleV = elePtrV.Elem()
		}
		if err := rows.Scan(elePtr); err != nil {
			return nil, err
		}
		if doAppend {
			sliceV = reflect.Append(sliceV, eleV)
		}
	}
	return &sliceV, rows.Err()
}
