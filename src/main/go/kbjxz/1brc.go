package main

import (
	"bytes"
	"context"
	"os"
	"time"

	"github.com/pkg/errors"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

type handler struct {
	f           *os.File
	fileName    string
	fileSize    int64
	readProcs   int64
	parseProces int64
	chunkSize   int64
	arena       chan []byte
}

type fileSlice struct {
	start, end int64
}

func newHandler(fileName string, chunkSize int64, readProcs, parseProcs int) (handler, error) {
	assert(readProcs > 0, "[readprocs] exp > 0, got: %d", readProcs)
	assert(parseProcs > 0, "[parseprocs] exp > 0, got: %d", parseProcs)

	var ret = handler{
		fileName:    fileName,
		readProcs:   int64(readProcs),
		parseProces: int64(parseProcs),
		arena:       make(chan []byte, parseProcs+1),
	}

	// open file
	f, err := os.Open(fileName)
	if err != nil {
		return ret, errors.WithStack(err)
	}
	ret.f = f

	// get file size
	stat, err := f.Stat()
	if err != nil {
		return ret, errors.WithStack(err)
	}
	ret.fileSize = stat.Size()

	// calc total number of chunks
	const maxReadBytes = 1 * GB
	assert(0 < chunkSize && chunkSize <= maxReadBytes, "invalid chunk size")
	ret.chunkSize = chunkSize

	for i := 0; i < readProcs+1; i++ {
		ret.arena <- make([]byte, chunkSize)
	}

	return ret, nil
}

func sliceFile(h *handler, latencies *[]time.Duration, put chan<- fileSlice) error {
	var peekBuf [64]byte
	var start int64
	for {
		beg := time.Now()

		if start+h.chunkSize >= h.fileSize {
			put <- fileSlice{start, h.fileSize}
			break
		}

		// find last line break in possible chunk
		var peekOffset = start + h.chunkSize - int64(len(peekBuf))
		gotOffset, err := h.f.Seek(peekOffset, 0)
		if err != nil {
			return errors.Wrapf(err, "peek: %d", peekOffset)
		}
		assert(gotOffset == peekOffset, "[peek] exp: %d, got: %d", peekOffset, gotOffset)

		n, err := h.f.Read(peekBuf[:])
		if err != nil {
			return errors.Wrapf(err, "read@%d", peekOffset)
		}

		lineBreak := bytes.LastIndexByte(peekBuf[:n], '\n')
		assert(lineBreak != -1, "lineBreak not found in [%d, %d)! peek buf may be too small",
			peekOffset, peekOffset+int64(n))
		end := peekOffset + int64(lineBreak) + 1
		*latencies = append(*latencies, time.Since(beg))
		put <- fileSlice{start, end}

		start = end
	}

	return nil
}

func readFileSlice(
	ctx context.Context, h *handler, latencies *[]time.Duration,
	put chan<- []byte, get <-chan fileSlice) error {

	f, err := os.Open(h.fileName)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	var fs fileSlice
	var ok bool
	for {
		select {
		case <-ctx.Done():
			return nil
		case fs, ok = <-get:
			if !ok {
				return nil
			}
		}

		beg := time.Now()
		gotStart, err := f.Seek(fs.start, 0)
		if err != nil {
			return errors.Wrapf(err, "seek failed at %+v", fs)
		}
		assert(gotStart == fs.start, "[seek] exp: %d, got: %d", fs.start, gotStart)

		buf := <-h.arena
		size := fs.end -fs.start
		n, err := f.Read(buf[:size])
		if err != nil {
			return errors.Wrapf(err, "[read] failed at %+v", fs)
		}
		assert(int64(n) == size, "[read.n] exp: %d, got: %d, at %+v",
			size, n, fs)
		*latencies = append(*latencies, time.Since(beg))

		put <- buf[:n]
	}
}
