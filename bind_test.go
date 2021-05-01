package sqlx

import (
	"fmt"
	"testing"
)

func Test_bindParams(t *testing.T) {
	fmt.Println(bindParams(DriverTypeMysql, "select * from user where id=${id} and name='${aaa}'"))
}
