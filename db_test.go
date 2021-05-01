package sqlx

import (
	"context"
	"fmt"
	"log"
	"testing"
)
import _ "github.com/lib/pq"

var db *DB

func init() {
	db, _ = Open("postgres", "postgres://postgres:123456@127.0.0.1:5432/sha")
	db.SetLogger(log.Default())
}

func TestDB_GetToMap(t *testing.T) {
	var m map[string]interface{}
	err := db.Get(context.Background(), "select ${a}::int+${b}::int as c", Params{"a": 45, "b": 459}, &m)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(m)
}

func TestDB_GetToStruct(t *testing.T) {
	type M struct {
		C int64 `db:"c"`
	}

	var m M
	err := db.Get(context.Background(), "select ${a}::int+${b}::int as c", Params{"a": 45, "b": 459}, &m)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(m)
}

func TestDB_SelectToMapValueSlice(t *testing.T) {
	var m []map[string]interface{}
	err := db.Select(context.Background(), "select ${a}::int+${b}::int as c", Params{"a": 45, "b": 459}, &m)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(m)
}

func TestDB_SelectToMapPtrSlice(t *testing.T) {
	var m []*map[string]interface{}
	err := db.Select(context.Background(), "select ${a}::int+${b}::int as c", Params{"a": 45, "b": 459}, &m)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(m[0])
}

func TestDB_SelectToStructValueSlice(t *testing.T) {
	type M struct {
		C int64 `db:"c"`
	}

	var m []M
	err := db.Select(context.Background(), "select ${a}::int+${b}::int as c", Params{"a": 45, "b": 459}, &m)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(m)
}

func TestDB_SelectToStructPtrSlice(t *testing.T) {
	type M struct {
		C int64 `db:"c"`
	}

	var m []*M
	err := db.Select(context.Background(), "select ${a}::int+${b}::int as c", Params{"a": 45, "b": 459}, &m)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(m[0])
}

func TestDB_Rows(t *testing.T) {
	var sum int64
	var dist DirectDists
	dist = append(dist, &sum)
	rows, err := db.Rows(context.Background(), "select ${a}::int+${b}::int as c", Params{"a": 45, "b": 459})
	if err != nil {
		panic(err)
	}
	defer rows.Close()
}

type A struct {
	Id   int64  `db:"id"`
	Name string `db:"name"`
}

type B struct {
	Id   int64  `db:"id"`
	Name string `db:"name"`
	AID  int64  `db:"aid"`
	A    *A     `db:"-"`
}

func (b *B) JoinIndex(i int) interface{} {
	switch i {
	case 0:
		return b
	case 1:
		ptr := &A{}
		b.A = ptr
		return ptr
	default:
		return nil
	}
}

func TestDB_GetJoined(t *testing.T) {
	var result B
	err := db.GetJoined(
		context.Background(),
		"select * from b, a where b.aid=a.id and b.name=${name}", Params{"name": "b1"},
		&result,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(result, result.A)
}

func TestDB_SelectJoined_Ptr(t *testing.T) {
	var result []*B
	err := db.SelectJoined(
		context.Background(),
		"select * from b, a where b.aid=a.id and b.name=${name}", Params{"name": "b1"},
		&result,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(result[0])
}

func TestDB_SelectJoined_Value(t *testing.T) {
	var result []B
	err := db.SelectJoined(
		context.Background(),
		"select * from b, a where b.aid=a.id and b.name=${name}", Params{"name": "b1"},
		&result,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(result[0])
}
