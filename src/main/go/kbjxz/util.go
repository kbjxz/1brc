package main

import "encoding/json"

func must[T any](v T, err error) T {
	must1(err)
	return v
}

func must1(err error) {
	if err != nil {
		panic(err)
	}
}

func assert(cond bool, expect string) {
	if !cond {
		panic(expect)
	}
}

func render(v any) string {
	data, _ := json.MarshalIndent(v, "", "    ")
	return string(data)
}
