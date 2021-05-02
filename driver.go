package sqlx

import (
	"fmt"
)

type DriverType int

const (
	DriverTypeUnknown = DriverType(iota)
	DriverTypePostgres
	DriverTypeMysql
	DriverTypeSqlite3
)

func (t DriverType) PlaceholderFunc() func(idx int, name string) string {
	switch t {
	case DriverTypeMysql, DriverTypeSqlite3:
		return func(_ int, _ string) string { return "?" }
	case DriverTypePostgres:
		return func(idx int, _ string) string { return fmt.Sprintf("$%d", idx+1) }
	default:
		panic(fmt.Errorf("sqlx: unsupported driver"))
	}
}

func nameToDriverType(name string) DriverType {
	switch name {
	case "mysql":
		return DriverTypeMysql
	case "postgres":
		return DriverTypePostgres
	case "sqlite3":
		return DriverTypeSqlite3
	default:
		panic(fmt.Errorf("sqlx: unsupported driver"))
	}
}
