# sqlx

a simple extension for [`database/sql`](https://golang.org/pkg/database/sql/), inspired by [`jmoiron/sqlx`](https://github.com/jmoiron/sqlx).

sub-package `reflectx` is copied from [`jmoiron/sqlx`](https://github.com/jmoiron/sqlx).

# content

- [open connection](#open)
- [execute sql](#execute)
- [select](#select)
- [tx](#tx)

# open

```go
db, _ = Open("postgres", "postgres://postgres:123456@127.0.0.1:5432/postgres")
db.SetLogger(log.Default())
```

# execute

```go

type Arg{
    Pwd string `db:"pwd"`
    Name string `db:"name"`
}

result, err := db.Execute(context.Background(), "update user set password=${pwd} where name=${name}", Arg{Pwd:"123456", Name:"ztk"})
```

# select

## select one raw to map/struct

```go
type Arg{
    A int `db:"a"`
    B int `db:"b"`
}

var result map[string]interface ()
//type Result{
//  C int `db:"c"`
//}
//var result Result

err := db.Get(context.Background(), "select ${a}::int+${b}::int as c", Arg{A: 45, B:79}, &result)
if err != nil{
    fmt.Println(result)
}
```

## select many raws to []map/[]struct/[]*map/[]*struct

```go
type Arg{
    A int `db:"a"`
    B int `db:"b"`
}

var result []map[string]interface ()
//type Result{
//	C int `db:"c"`
//}
//var result []Result

err := db.Get(context.Background(), "select ${a}::int+${b}::int as c", Arg{A: 45, B:79}, &result)
if err != nil{
fmt.Println(result)
}
```

## select some specific columns of one row

```go
type Arg{
    A int `db:"a"`
    B int `db:"b"`
}

var result int
var dist DirectDist
dist = append(dist, &result)

rows, err := db.Rows(context.Background(), "select ${a}::int+${b}::int as c", Arg{A: 45, B:79}, &dist)
if err != nil{
    fmt.Println(result)
}
```

# tx

## begin and commit

```go
tx := db.MustBeginTx(context.Background(), nil)
defer tx.AutoCommit()

tx.Execute(...)
```

## nested tx(aka, savepoint)

```go
tx := db.MustBeginTx(context.Background(), nil)
defer tx.AutoCommit()

ntx := tx.MustBegin(context.Background(), "SavepointName")
defer ntx.AutoCommit()
```
