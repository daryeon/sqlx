package sqlx

import (
	"fmt"
	"testing"
)

func Test_paramsToMap(t *testing.T) {
	type Arg struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}

	type ArgV struct {
		Arg
		V bool `db:"v"`
	}

	fmt.Println(paramsToMap(ArgV{Arg: Arg{Name: "ztk", Age: 67}, V: true}))
	fmt.Println(paramsToMap(map[string]interface{}{"X": 45}))
	fmt.Println(paramsToMap(Params{"x": 566}))

	fmt.Println(ParamsToArgs(ArgV{Arg: Arg{Name: "ztk", Age: 67}, V: true}, []string{"name"}))
	fmt.Println(ParamsToArgs(map[string]interface{}{"X": 45}, []string{"X"}))
	fmt.Println(ParamsToArgs(Params{"X": 45}, []string{"X"}))
}
