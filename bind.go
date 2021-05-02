package sqlx

import (
	"strings"
)

type _Param struct {
	name  string
	begin int
	end   int
}

func scanParams(q []rune) []_Param {
	var lst []_Param
	begin := -1
	status := -1
	escape := false
	var quote rune
	var buf []rune
	for idx, r := range q {
		if escape {
			escape = false
			continue
		}
		if r == '\\' {
			escape = true
			continue
		}

		if quote != 0 {
			if r == quote {
				quote = 0
			}
			continue
		}

		if r == '"' || r == '\'' {
			quote = r
			continue
		}

		if status < 0 && r == '$' {
			status++
			begin = idx
			continue
		}

		switch status {
		case 0:
			if r == '{' {
				status++
			} else {
				status = -1
			}
			continue
		case 1:
			if r == '}' {
				status = -1
				lst = append(lst, _Param{name: string(buf), begin: begin, end: idx})
				buf = buf[:0]
			} else {
				buf = append(buf, r)
			}
		}
	}

	return lst
}

func BindParams(d DriverType, qs string) (string, []string) {
	if !strings.Contains(qs, "${") {
		return qs, nil
	}

	q := []rune(qs)
	lst := scanParams(q)
	if len(lst) < 1 {
		return qs, nil
	}

	var placeholder = d.PlaceholderFunc()

	var buf strings.Builder
	var keys []string
	cur := 0
	for idx, param := range lst {
		for cur < param.begin {
			buf.WriteRune(q[cur])
			cur++
		}

		buf.WriteString(placeholder(idx, param.name))
		keys = append(keys, param.name)
		cur = param.end + 1
	}

	for cur < len(q) {
		buf.WriteRune(q[cur])
		cur++
	}
	return buf.String(), keys
}
