package main

import (
	"context"
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
		size := r.end - r.start
		if size > chunkSize {
			t.Errorf("[%d] unexpected size! exp: %d, got: {start:%d, end:%d, size:%d}\n",
				i, chunkSize, r.start, r.end, size)
		} else {
			t.Logf("[%d]: {start:%d, end:%d, size:%d}\n", i, r.start, r.end, size)
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
