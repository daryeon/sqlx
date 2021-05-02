package sqlx

import (
	"fmt"
	"testing"
)

func Test_bindParams(t *testing.T) {
	fmt.Println(BindParams(DriverTypeMysql, "select * from user where id=${id} and name='${aaa}'"))
	fmt.Println(BindParams(DriverTypePostgres, "select * from user where id=${id} and name='${aaa}'"))
}
