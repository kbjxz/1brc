package main

import (
	"math/rand"
	"strings"
	"testing"
)

func Test_getMeta(t *testing.T) {
	ret, err := getMeta("./measurements.txt")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(render(ret))
}

var lineData = [][]byte{
	[]byte("Hamburg;12.0"),
	[]byte("Bulawayo;8.9"),
	[]byte("Palembang;38.8"),
	[]byte("St. John's;15.2"),
	[]byte("Cracow;12.6"),
	[]byte("Bridgetown;26.9"),
	[]byte("Istanbul;6.2"),
	[]byte("Roseau;34.4"),
	[]byte("Conakry;31.2"),
	[]byte("Istanbul;23.0"),
}

func Benchmark_parseLine(b *testing.B) {
	record := make([]_record, len(lineData))
	for b.Loop() {
		for i := range lineData {
			var err error
			record[i], err = _parseLine(lineData[i])
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	// b.Log(render(record))
}

func Benchmark_parseLine2(b *testing.B) {
	record := make([]record, len(lineData))
	var buf [8]byte
	for b.Loop() {
		for i := range lineData {
			var err error
			record[i], err = parseLine(lineData[i], buf)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	// b.Log(render(record))
}

func Test_parseLine2(t *testing.T) {
	record := make([]record, len(lineData))
	var buf [8]byte
	for i := range lineData {
		var err error
		record[i], err = parseLine(lineData[i], buf)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Log(render(record))
}

func Test_scanLines(t *testing.T) {
	var buf []byte
	for i := range lineData {
		buf = append(buf, lineData[i]...)
		if i+1 < len(lineData) {
			buf = append(buf, '\n')
		}
	}

	var result []string
	for len(buf) > 0 {
		var line []byte
		line, buf = scanLine(buf)
		result = append(result, string(line))
	}
	t.Log(strings.Join(result, "\n"))
}

func Benchmark_scanLines(b *testing.B) {
	var buf []byte
	for i := range lineData {
		buf = append(buf, lineData[i]...)
		if i+1 < len(lineData) {
			buf = append(buf, '\n')
		}
	}

	result := make([]string, len(lineData))
	for b.Loop() {
		for i := 0; len(buf) > 0; i++ {
			var line []byte
			line, buf = scanLine(buf)
			result[i] = string(line)
		}
	}
	b.Log(strings.Join(result, "\n"))
}

func makeLineData2(size int) [][]byte {
	ret := make([][]byte, size)
	n := len(lineData)
	for i := range ret {
		ret[i] = lineData[rand.Intn(n)]
	}
	return ret
}

var lineRecords2 = func() []record {
	lineData := makeLineData2(100000)
	record := make([]record, len(lineData))
	var buf [8]byte
	for i := range lineData {
		record[i] = must(parseLine(lineData[i], buf))
	}
	return record
}()

func Benchmark_insertRecord(b *testing.B) {
	for b.Loop() {
		pr := partialResult{
			Index: make(map[string]int, 50000),
			List:  make([]stationData, 50000),
		}
		for i := range lineRecords2 {
			pr.insert(&lineRecords2[i])
		}
	}
}

func Benchmark_insertRecord2(b *testing.B) {
	for b.Loop() {
		pr := make(_partialResult)
		for i := range lineRecords2 {
			pr.insert(&lineRecords2[i])
		}
	}
}

func makePartialLists(par, size int) [][]stationData {
	var buf [8]byte
	ret := make([][]stationData, par)
	for i := range ret {
		lineData := makeLineData2(size)
		plist := make([]stationData, len(lineData))
		for i, b := range lineData {
			r := must(parseLine(b, buf))
			plist[i] = stationData{
				Station: r.Station,
				Count:   1,
				Min:     r.Temp,
				Avg:     r.Temp,
				Max:     r.Temp,
			}
		}
		ret[i] = plist
	}
	return ret
}

var partialLists16_100000 = makePartialLists(16, 100000)

func Benchmark_reduce(b *testing.B) {
	// list := makePartialLists(3, 5)
	for b.Loop() {
		_reduceFinalResult(partialLists16_100000)
	}
}

func Benchmark_reduce2(b *testing.B) {
	for b.Loop() {
		reduceFinalResult(partialLists16_100000)
	}
}
