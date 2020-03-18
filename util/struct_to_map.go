package util

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
)

type strToMap struct {
	normalized map[string]string
}

func StrToMap(path string, v interface{}) [][]string {
	m := strToMap{normalized: make(map[string]string)}
	return m.sortAndConvert(path, v)
}

func (n *strToMap) sortAndConvert(parent string, v interface{}) [][]string {
	n.split(parent, reflect.ValueOf(v))

	return n.sort()
}

func (n *strToMap) sort() [][]string {
	var keyVals [][]string
	keys := make([]string, 0, len(n.normalized))
	for k := range n.normalized {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		keyVals = append(keyVals, []string{k, n.normalized[k]})
	}
	return keyVals
}

func (n *strToMap) split(parent string, v reflect.Value) {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.IsZero() {
		return
	}

	types := v.Type()

	for i := 0; i < v.NumField(); i++ {

		f := v.Field(i)
		//println(f.Kind())
		path := types.Field(i).Name

		if !f.CanInterface() {
			continue
		}

		if parent != `` {
			path = parent + `.` + path
		}

		if f.Kind() == reflect.Interface && f.IsNil() {
			n.normalized[path] = `<nil>`
			continue
		}

		if !(f.Kind() == reflect.Interface && f.IsNil()) && f.CanInterface() && f.NumMethod() > 0 {
			emty := reflect.Value{}
			if vv := f.MethodByName(`String`); vv != emty {
				n.normalized[path] = vv.Call(nil)[0].String()
				continue
			} else if vv := f.MethodByName(`Name`); vv != emty {
				n.normalized[path] = vv.Call(nil)[0].String()
				continue
			}
		}

		if f.Kind() == reflect.Ptr && f.CanInterface() {
			n.split(path, f)
			continue
		}

		if f.Kind() == reflect.Struct {
			n.split(path, f)
			continue
		}

		if path != `` {
			n.normalized[path] = n.toString(f)
		}
	}
}

func (n *strToMap) toString(value reflect.Value) string {
	switch value.Kind() {
	case reflect.Map, reflect.Array, reflect.Slice:
		return fmt.Sprintf(`%+v`, value)
	case reflect.Int, reflect.Int32, reflect.Int16, reflect.Int64:
		return fmt.Sprintf(`%d`, value.Int())
	case reflect.Bool:
		return fmt.Sprint(value.Bool())
	case reflect.Float64, reflect.Float32:
		return fmt.Sprint(value.Float())
	case reflect.Func:
		return runtime.FuncForPC(value.Pointer()).Name()
	default:
		return value.String()
	}
}
