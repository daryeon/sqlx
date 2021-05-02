package sqlx

import (
	"context"
	"errors"
	"fmt"
	"github.com/zzztttkkk/sqlx/reflectx"
	"reflect"
	"sort"
	"strings"
)

type Columns []string

type Model interface {
	TableName() string
	TableColumns() []string
}

type Operator struct {
	smap       *reflectx.StructMap
	groups     map[string]map[string]int
	immutables map[string]bool
	db         *DB
	model      Model
}

func (op *Operator) addGroup(group, column string, ind int) {
	gm := op.groups[group]
	if len(gm) < 1 {
		gm = map[string]int{}
		op.groups[group] = gm
	}
	gm[column] = ind
}

func NewOperator(db *DB, model Model) *Operator {
	t := reflect.TypeOf(model)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		panic(fmt.Errorf("sqlx: model should be a struct pointer"))
	}
	op := &Operator{
		smap:       mapper.TypeMap(t.Elem()),
		db:         db,
		model:      model,
		groups:     map[string]map[string]int{},
		immutables: map[string]bool{},
	}

	idx := 0
	for n, f := range op.smap.Names {
		groups := f.Options["group"]
		groups += "|*"
		for _, gn := range strings.Split(groups, "|") {
			gn = strings.TrimSpace(gn)
			if len(gn) < 1 {
				continue
			}
			op.addGroup(gn, n, idx)
		}
		if len(f.Options["immutable"]) > 0 {
			op.immutables[n] = true
		}

		idx++
	}
	return op
}

func (op *Operator) Group(group string, order bool) []string {
	gm := op.groups[group]
	if len(gm) < 1 {
		return nil
	}

	var lst []string
	if order {
		type _NI struct {
			s string
			i int
		}
		nis := make([]_NI, 0, len(gm))
		for k, i := range gm {
			nis = append(nis, _NI{s: k, i: i})
		}
		sort.Slice(nis, func(i, j int) bool { return nis[i].i < nis[j].i })
		for _, v := range nis {
			lst = append(lst, v.s)
		}
	} else {
		for k := range gm {
			lst = append(lst, k)
		}
	}
	return lst
}

func (op *Operator) IsImmutable(name string) bool {
	return op.immutables[name]
}

func (op *Operator) CreateTable(ctx context.Context) error {
	var buf strings.Builder
	buf.WriteString("CREATE TABLE IF NOT EXISTS ")
	buf.WriteString(op.model.TableName())
	buf.WriteString(" (")
	buf.WriteString(strings.Join(op.model.TableColumns(), ", "))
	buf.WriteString(")")
	_, e := op.db.Execute(ctx, buf.String(), nil)
	return e
}

var ErrEmptyData = errors.New("sqlx: empty data")
var ErrEmptyCondition = errors.New("sqlx: empty condition")

func (op *Operator) SqlSelect(groupOrKeys string, condition string) string {
	if len(condition) < 1 {
		panic(ErrEmptyCondition)
	}

	var buf strings.Builder
	buf.WriteString("SELECT ")
	if len(groupOrKeys) < 1 || groupOrKeys == "*" {
		buf.WriteString("*")
	} else {
		if groupOrKeys[0] == '!' {
			buf.WriteString(groupOrKeys[1:])
		} else {
			buf.WriteString(strings.Join(op.Group(groupOrKeys, true), ","))
		}
	}
	buf.WriteString(" FROM ")
	buf.WriteString(op.model.TableName())
	buf.WriteString(" WHERE ")
	buf.WriteString(condition)
	return buf.String()
}

func (op *Operator) Get(
	ctx context.Context, groupOrKeys string, condition string,
	params interface{},
	dist interface{},
) error {
	ctx, exe := PickExecutor(ctx)
	return exe.Get(ctx, op.SqlSelect(groupOrKeys, condition), params, dist)
}

func (op *Operator) Select(
	ctx context.Context, groupOrKeys string, condition string,
	params interface{},
	dist interface{},
) error {
	_, exe := PickExecutor(ctx)
	return exe.Select(ctx, op.SqlSelect(groupOrKeys, condition), params, dist)
}

type Returning struct {
	Keys  []string
	Dists DirectDists
}

func (op *Operator) SqlInsert(columns []string, returning *Returning) string {
	if len(columns) < 1 {
		panic(ErrEmptyData)
	}

	var buf strings.Builder
	buf.WriteString("INSERT INTO ")
	buf.WriteString(op.model.TableName())
	buf.WriteByte('(')
	end := len(columns) - 1
	ind := 0
	for _, k := range columns {
		buf.WriteString(k)
		if ind < end {
			buf.WriteByte(',')
		}
		ind++
	}
	buf.WriteString(") VALUES(")
	ind = 0
	for _, k := range columns {
		buf.WriteString("${")
		buf.WriteString(k)
		buf.WriteByte('}')
		if ind < end {
			buf.WriteByte(',')
		}
		ind++
	}
	buf.WriteByte(')')

	if returning != nil {
		buf.WriteString(" RETURNING ")
		buf.WriteString(strings.Join(returning.Keys, ","))
	}
	return buf.String()
}

func (op *Operator) Insert(ctx context.Context, params interface{}, returning *Returning) (int64, error) {
	pm, err := paramsToMap(params)
	if err != nil {
		return 0, err
	}

	_, exe := PickExecutor(ctx)
	if returning == nil {
		r, e := exe.Execute(ctx, op.SqlInsert(pm.Keys(), returning), pm)
		if e != nil {
			return 0, e
		}
		return r.LastInsertId()
	}
	return 0, exe.GetDirect(ctx, op.SqlInsert(pm.Keys(), returning), pm, returning.Dists)
}

func (op *Operator) SqlUpdate(condition string, columns []string, returning *Returning) string {
	if len(columns) < 1 {
		panic(ErrEmptyData)
	}
	if len(condition) < 1 {
		panic(ErrEmptyCondition)
	}

	var buf strings.Builder
	buf.WriteString("UPDATE ")
	buf.WriteString(op.model.TableName())
	buf.WriteString(" SET ")
	end := len(columns) - 1
	ind := 0
	for _, k := range columns {
		buf.WriteString(k)
		buf.WriteString("=${")
		buf.WriteString(k)
		buf.WriteByte('}')
		if ind < end {
			buf.WriteByte(',')
		}
		ind++
	}
	buf.WriteString(" WHERE ")
	buf.WriteString(condition)
	if returning != nil {
		buf.WriteString(" RETURNING ")
		buf.WriteString(strings.Join(returning.Keys, ","))
	}
	return buf.String()
}

func (op *Operator) Update(ctx context.Context, condition string, data, params interface{}, returning *Returning) (int64, error) {
	pm, err := paramsToMap(params)
	if err != nil {
		return 0, err
	}
	dm, err := paramsToMap(data)
	if err != nil {
		return 0, err
	}
	for k, v := range dm {
		pm[k] = v
	}
	_, exe := PickExecutor(ctx)
	if returning == nil {
		r, e := exe.Execute(ctx, op.SqlUpdate(condition, dm.Keys(), returning), pm)
		if e != nil {
			return 0, e
		}
		return r.RowsAffected()
	}
	return 0, exe.GetDirect(ctx, op.SqlUpdate(condition, dm.Keys(), returning), pm, returning.Dists)
}

func (op *Operator) SqlDelete(condition string) string {
	if len(condition) < 1 {
		panic(ErrEmptyCondition)
	}

	var buf strings.Builder
	buf.WriteString("DELETE FROM ")
	buf.WriteString(op.model.TableName())
	buf.WriteString(" WHERE ")
	buf.WriteString(condition)
	return buf.String()
}

func (op *Operator) Delete(ctx context.Context, condition string, params interface{}) (int64, error) {
	_, exe := PickExecutor(ctx)
	r, e := exe.Execute(ctx, op.SqlDelete(condition), params)
	if e != nil {
		return 0, e
	}
	return r.RowsAffected()
}
