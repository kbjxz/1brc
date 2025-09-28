package main

import (
	"encoding/json"
	"fmt"
)

func must[T any](v T, err error) T {
	must1(err)
	return v
}

func must1(err error) {
	if err != nil {
		panic(err)
	}
}

func assert(cond bool, expect string, args ...any) {
	if !cond {
		panic(fmt.Sprintf(expect, args...))
	}
}

func render(v any) string {
	data, _ := json.MarshalIndent(v, "", "    ")
	return string(data)
}
