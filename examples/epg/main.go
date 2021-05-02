package main

import (
	"context"
	"fmt"
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
	ID   int64  `db:"id"`
	Name string `db:"name"`
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
	Id       int64  `db:"id"`
	Title    string `db:"title"`
	Content  string `db:"content"`
	AuthorID int64  `db:"author"`
	Author   User   `db:"-"`
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

func selectArticleWithAuthor(ctx context.Context) {
	ctx, tx := sqlx.BeginTx(ctx, nil)
	defer tx.AutoCommit()

	var result = make([]Article, 0, 100)
	err := tx.SelectJoined(
		ctx,
		"select * from content_article as A, account_user as U where A.author=U.id order by A.title limit ${limit}",
		sqlx.ParamSlice{100},
		&result,
		func(raw interface{}, idx int) (interface{}, int) {
			v := raw.(*Article)
			switch idx {
			case 0:
				return v, -1
			case 1:
				return &v.Author, -1
			default:
				return nil, -1
			}
		},
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < len(result); i++ {
		article := &(result[i])
		fmt.Printf("Title: %s, Author: %s\n", article.Title, article.Author.Name)
	}
}

func main() {
	//createUser(context.Background())
	//createArticle(context.Background())
	selectArticleWithAuthor(context.Background())
}
