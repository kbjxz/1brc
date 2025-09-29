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
	err = sliceFile(&h, ch)
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
	eg, ctx := errgroup.WithContext(context.Background())
	eg.Go(func() error {
		defer close(fromSliceToRead)
		return sliceFile(&h, fromSliceToRead)
	})

	var latencies []time.Duration
	var fromReadToParse = make(chan []byte)
	eg.Go(func() error {
		defer close(fromReadToParse)
		return readFileSlice(ctx, &h, &latencies, fromReadToParse, fromSliceToRead)
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
}
