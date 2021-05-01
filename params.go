package sqlx

import (
	"fmt"
	"reflect"
)

type Params map[string]interface{}

var paramsType = reflect.TypeOf(Params{})
var mapType = reflect.TypeOf(map[string]interface{}{})

func paramsToMap(params interface{}) (Params, error) {
	t := reflect.TypeOf(params)
	if t == paramsType {
		return params.(Params), nil
	}
	if t == mapType {
		return params.(map[string]interface{}), nil
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("sqlx: bad params value `%v`", params)
	}

	var m Params
	var pv = reflect.ValueOf(params)
	for n, f := range mapper.TypeMap(t).Names {
		fv := pv.FieldByIndex(f.Index)
		if m == nil {
			m = make(Params)
		}
		m[n] = fv.Interface()
	}
	return m, nil
}

func paramsToArgs(params interface{}, keys []string) ([]interface{}, error) {
	t := reflect.TypeOf(params)
	if t == paramsType {
		var args []interface{}
		m := params.(Params)
		for _, k := range keys {
			v, ok := m[k]
			if !ok {
				return nil, fmt.Errorf("sqlx: missing key `%s`", k)
			}
			args = append(args, v)
		}
		return args, nil
	}

	if t == mapType {
		var args []interface{}
		m := params.(map[string]interface{})
		for _, k := range keys {
			v, ok := m[k]
			if !ok {
				return nil, fmt.Errorf("sqlx: missing key `%s`", k)
			}
			args = append(args, v)
		}
		return args, nil
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("sqlx: bad params value `%v`", params)
	}

	var args []interface{}
	var m = mapper.TypeMap(t).Names
	var pv = reflect.ValueOf(params)
	for _, k := range keys {
		f, ok := m[k]
		if !ok {
			return nil, fmt.Errorf("sqlx: missing key `%s`", k)
		}
		fv := pv.FieldByIndex(f.Index)
		args = append(args, fv.Interface())
	}
	return args, nil
}
