package sqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/zzztttkkk/sqlx/reflectx"
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
	dt := reflect.TypeOf(dist)
	if dt == distsType {
		return rows.Rows.Scan(dist.(DirectDists)...)
	}

	v := reflect.ValueOf(dist).Elem()
	t := v.Type()
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

type JoinedDist interface {
	JoinIndex(idx int) interface{}
}

func (rows *Rows) ScanJoined(dist JoinedDist) error {
	v := reflect.ValueOf(dist).Elem()
	t := v.Type()
	if t.Kind() != reflect.Struct {
		return ErrUnexpectedDistType
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	dIdx := 0
	var d interface{}
	var dV reflect.Value
	var dT *reflectx.StructMap
	var dN int

	var ptrs []interface{}
	for _, c := range columns {
		if dT == nil {
			d = dist.JoinIndex(dIdx)
			dV = reflect.ValueOf(d).Elem()
			dT = mapper.TypeMap(reflect.TypeOf(d).Elem())
			dN = len(dT.Names)
		}
		fi, ok := dT.Names[c]
		if !ok {
			return fmt.Errorf("sha.sqlx: bad column name `%s`", c)
		}
		f := dV.FieldByIndex(fi.Index)
		ptrs = append(ptrs, f.Addr().Interface())
		dN--
		if dN == 0 {
			dT = nil
			dIdx++
		}
	}
	return rows.Rows.Scan(ptrs...)
}

func (rows *Rows) get(dist interface{}) error {
	for rows.Next() {
		err := rows.Scan(dist)
		if err != nil {
			return err
		}
		return nil
	}
	return rows.Err()
}

func (rows *Rows) _select(slicePtr interface{}) error {
	var err error
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

func (rows *Rows) getJoined(dist JoinedDist) error {
	for rows.Next() {
		err := rows.ScanJoined(dist)
		if err != nil {
			return err
		}
		return nil
	}
	return rows.Err()
}

func (rows *Rows) selectJoined(slicePtr interface{}) error {
	var err error
	sliceV := reflect.ValueOf(slicePtr).Elem()
	eleT := sliceV.Type().Elem()
	isPtrSlice := false
	if eleT.ConvertibleTo(joinedDistType) {
		if eleT.Kind() != reflect.Ptr {
			return ErrUnexpectedDistType
		}
		isPtrSlice = true
	} else {
		if eleT.Kind() != reflect.Struct {
			return ErrUnexpectedDistType
		}
		elePT := reflect.New(eleT).Type()
		if !elePT.ConvertibleTo(joinedDistType) {
			return ErrUnexpectedDistType
		}
	}

	for rows.Next() {
		var eleV reflect.Value
		doAppend := true
		if isPtrSlice {
			eleV = reflect.New(eleT.Elem())
		} else {
			l := sliceV.Len() + 1
			if l < sliceV.Cap() {
				doAppend = false
				sliceV.SetLen(l)
				eleV = sliceV.Index(l - 1).Addr()
			} else {
				eleV = reflect.New(eleT)
			}
		}

		err = rows.ScanJoined(eleV.Interface().(JoinedDist))
		if err != nil {
			return err
		}

		if doAppend {
			if isPtrSlice {
				sliceV = reflect.Append(sliceV, eleV)
			} else {
				sliceV = reflect.Append(sliceV, eleV.Elem())
			}
		}
	}
	reflect.ValueOf(slicePtr).Elem().Set(sliceV)
	return rows.Err()
}
