package main

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

var testParams = &params{
	fileName:  "./measurements.txt",
	procs:     1,
	chunkSize: 1 * GB,
}

func Test_getMeta(t *testing.T) {

	ret, err := getMeta(testParams)
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

// func Benchmark_readRegion_singleThread(b *testing.B) {
// 	meta := must(getMeta(testParams))
// 	for _, chunkSize := range []int64{
// 		4 * KB,
// 		// 16 * KB,
// 		// 1 * MB,
// 		4 * MB,
// 		256 * MB,
// 		1 * GB,
// 	} {
// 		b.Run(sprintSize(chunkSize), func(b *testing.B) {
// 			for b.Loop() {
// 				meta.ChunkSize = chunkSize
// 				readRegion(&meta, 0)
// 			}
// 		})
// 	}
// }

func sprintSize(size int64) string {
	prec := func(v, unit int64) string {
		return fmt.Sprintf("%.1f", float64(v*10/unit)/10.0)
	}

	if size >= GB {
		return fmt.Sprint(prec(size, GB), "GB")
	} else if size >= MB {
		return fmt.Sprint(prec(size, MB), "MB")
	} else if size >= KB {
		return fmt.Sprint(prec(size, KB), "KB")
	} else {
		return fmt.Sprint(prec(size, 1), "B")
	}
}

// func Benchmark_readRegion_multiThread(b *testing.B) {
// 	for _, procs := range []int{
// 		1, 4, 16, 64,
// 	} {
// 		b.Run(strconv.Itoa(procs), func(b *testing.B) {
// 			params := *testParams
// 			params.procs = procs
// 			meta := must(getMeta(&params))
// 			for b.Loop() {
// 				for i := range meta.Regions {
// 					i := i
// 					readRegion(&meta, i)
// 				}
// 			}
// 		})
// 	}
// }

func newCyclicBytesLeakyBuffer(n, size int) chan []byte {
	ch := make(chan []byte, n)
	for i := 0; i < n; i++ {
		ch <- make([]byte, size)
	}
	return ch
}

func Benchmark_readFileChunks(b *testing.B) {
	meta := must(getMeta(testParams))
	put := make(chan []byte)
	defer close(put)
	get := newCyclicBytesLeakyBuffer(3, GB)
	go func() {
		tailBuf := [32]byte{}
		i := 0
		for chunk := range put {
			tailSize := min(len(chunk), len(tailBuf))
			copy(tailBuf[:], chunk[len(chunk)-tailSize:])
			fmt.Printf("[%d] %s\n", i, 
				strings.ReplaceAll(string(tailBuf[:tailSize]), "\n", "\\n"))
			i++
			get <- chunk
		}
	}()
	for b.Loop() {
		err := readFileChunks(&meta, put, get)
		if err != nil {
			b.Fatalf("%+v", err)
		}
	}
}
