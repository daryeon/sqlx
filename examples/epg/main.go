package main

import (
	"context"
	_ "github.com/lib/pq"
	"github.com/zzztttkkk/sqlx"
	"log"
	"math/rand"
	"time"
)

var DB *sqlx.DB

func init() {
	var err error
	DB, err = sqlx.OpenWriteableDB("postgres", "user=postgres password=123456 database=testing")
	if err != nil {
		panic(err)
	}
	DB.SetLogger(log.Default())
}

type User struct {
	id   int64  `db:"id"`
	name string `db:"name"`
}

func (user *User) TableName() string { return "account_user" }

func (user *User) TableColumns() []string {
	return []string{
		"id serial8 not null primary key",
		"name char(20) not null unique",
	}
}

var UserOperator *sqlx.Operator

type Article struct {
	id       int64  `db:"id"`
	title    string `db:"title"`
	content  string `db:"content"`
	authorID int64  `db:"author"`
	author   User   `db:"-"`
}

func (article *Article) TableName() string { return "content_article" }

func (article *Article) TableColumns() []string {
	return []string{
		"id serial8 not null primary key",
		"title char(50) not null",
		"content text not null",
		"author bigint not null",
	}
}

var ArticleOperator *sqlx.Operator

func init() {
	UserOperator = sqlx.NewOperator(DB, &User{})
	ArticleOperator = sqlx.NewOperator(DB, &Article{})

	UserOperator.CreateTable(context.Background())
	ArticleOperator.CreateTable(context.Background())

	rand.Seed(time.Now().UnixNano())
}

func randChinese(size int) string {
	buf := make([]rune, size, size)
	for i := 0; i < size; i++ {
		buf[i] = rune(19968 + rand.Int63n(40869-19968))
	}
	return string(buf)
}

func createUser(ctx context.Context) {
	ctx, tx := sqlx.BeginTx(ctx, nil)
	defer tx.AutoCommit()

	stmt, err := tx.Prepare(ctx, UserOperator.SqlInsert(sqlx.Columns{"name"}, nil))
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	params := make(sqlx.ParamSlice, 1, 1)
	for i := 0; i < 100; i++ {
		params[0] = randChinese(10)
		_, e := stmt.Execute(ctx, params)
		if e != nil {
			panic(e)
		}
	}
}

func createArticle(ctx context.Context) {
	ctx, tx := sqlx.BeginTx(ctx, nil)
	defer tx.AutoCommit()

	stmt, err := tx.Prepare(
		ctx,
		"insert into content_article (title,content,author) values (${title}, ${content}, ${author_id})",
	)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	var maxUserID int64
	_ = tx.GetDirect(ctx, "select id from account_user order by id desc limit 1", nil, sqlx.DirectDists{&maxUserID})

	params := make(sqlx.ParamSlice, 3, 3)
	for i := 0; i < 100; i++ {
		params[0] = randChinese(10)
		params[1] = randChinese(100)
		params[2] = (int64(rand.Int()) % maxUserID) + 1
		_, e := stmt.Execute(ctx, params)
		if e != nil {
			panic(e)
		}
	}
}

func main() {
	createUser(context.Background())
	createArticle(context.Background())
}
