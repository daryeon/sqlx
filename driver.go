package sqlx

import "fmt"

type DriverType int

const (
	DriverTypeUnknown = DriverType(iota)
	DriverTypePostgres
	DriverTypeMysql
)

func postgresPlaceholder(idx int) string { return fmt.Sprintf("$%d", idx+1) }

func mysqlPlaceholder(idx int) string { return "?" }
