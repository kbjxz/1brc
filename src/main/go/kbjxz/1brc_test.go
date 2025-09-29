package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func Test_sliceFile(t *testing.T) {
	const chunkSize = 250 * MB
	h, err := newHandler("./measurements.txt", chunkSize, 1, 1)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var fileSlices []fileSlice
	var ch = make(chan fileSlice)
	go func() {
		for r := range ch {
			fileSlices = append(fileSlices, r)
		}
	}()
	var latencies []time.Duration
	err = sliceFile(&h, &latencies, ch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for i, r := range fileSlices {
		size := r.end - r.beg
		if size > chunkSize {
			t.Errorf("[%d] unexpected size! exp: %d, got: {start:%d, end:%d, size:%d}\n",
				i, chunkSize, r.beg, r.end, size)
		} else {
			t.Logf("[%d]: {start:%d, end:%d, size:%d}\n", i, r.beg, r.end, size)
		}
	}
}

func Test_readFileSlice(t *testing.T) {
	const chunkSize = 250 * MB
	h, err := newHandler("./measurements.txt", chunkSize, 1, 1)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var fromSliceToRead = make(chan fileSlice)
	var sliceLatencies = make([]time.Duration, 0, h.fileSize/h.chunkSize+1)
	eg, ctx := errgroup.WithContext(context.Background())
	eg.Go(func() error {
		defer close(fromSliceToRead)
		return sliceFile(&h, &sliceLatencies, fromSliceToRead)
	})

	var readLatencies = make([]time.Duration, 0, cap(sliceLatencies))
	var fromReadToParse = make(chan []byte)
	eg.Go(func() error {
		defer close(fromReadToParse)
		return readFileSlice(ctx, &h, &readLatencies, fromReadToParse, fromSliceToRead)
	})

	eg.Go(func() error {
		for b := range fromReadToParse {
			h.arena <- b
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		t.Fatalf("%+v", err)
	}

	t.Logf("[latency.slice] total: %v, details: %+v", sumDurations(sliceLatencies), sliceLatencies)
	t.Logf("[latency.read] total: %v, details: %+v", sumDurations(readLatencies), readLatencies)
}

func Test_parseChunk(t *testing.T) {
	var ctx = context.Background()
	var h = handler{
		arena: make(chan []byte, 1),
	}
	var eg errgroup.Group
	var ch = make(chan []byte)
	var parseLatencies = []time.Duration{}
	eg.Go(func() error {
		return parseChunk(ctx, &h, &parseLatencies, ch)
	})
	ch <- []byte(`Gagarin Shahri;58.2
Miracema;-71.3
Sárospatak;56.7
Oxford;-79.9
Anajatuba;-54.5
Shitāb Diāra;20.9
Gusinje;-71.1
Ārambākkam;-27.4
Tarrafal;-10.6
Goleniów;23.7
Bhāgsar;-14.6
Saltillo;-47.2
Töle Bī;63.3
Ingleside;63.2
Peristéri;-44.6
Imsida;27.8
Evensk;-55.2
Derecik;23.9
Someren;87.1
San Juan Lalana;-34.3
Madakasīra;54.1
Zacatelco;-76.5
Dieppe;38.4
Ertil;83.3
General Viamonte;46.8
Meiningen;-62.0
Kakata;86.2`)

	if err := eg.Wait(); err != nil {
		t.Fatalf("%+v", err)
	}

	t.Logf("[latency.parse] total: %v, details: %+v", sumDurations(parseLatencies), parseLatencies)
}

func Test_parseLine2_(t *testing.T) {
	var records = []string{
		`Gagarin Shahri;58.2`,
		`Miracema;-71.3`,
		`Sárospatak;56.7`,
	}
	var data = []byte(strings.Join(records, "\n"))
	var exp = []parseResult{
		{"Gagarin Shahri", 582, len(records[0]) + 1},
		{`Miracema`, -713, len(records[1]) + 1},
		{`Sárospatak`, 567, len(records[2])},
	}
	var got = []parseResult{}

	for len(data) > 0 {
		pr, err := parseLine2(data)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		got = append(got, pr)
		data = data[pr.advacend:]
	}

	if !reflect.DeepEqual(got, exp) {
		t.Errorf("\nexp:%+v\ngot:%+v", exp, got)
	}
}
