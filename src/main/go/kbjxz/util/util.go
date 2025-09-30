package util

import (
	"encoding/json"
	"fmt"
	"time"
	"unsafe"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

func Must[T any](v T, err error) T {
	Must1(err)
	return v
}

func Must1(err error) {
	if err != nil {
		panic(err)
	}
}

func Assert(cond bool, expect string, args ...any) {
	if !cond {
		panic(fmt.Sprintf(expect, args...))
	}
}

func Render(v any) string {
	data, _ := json.MarshalIndent(v, "", "    ")
	return string(data)
}

func SumDurations(ds []time.Duration) time.Duration {
	var total time.Duration
	for _, v := range ds {
		total += v
	}
	return total
}

func ResetChunk(b []byte) []byte {
	return unsafe.Slice(unsafe.SliceData(b), cap(b))
}
