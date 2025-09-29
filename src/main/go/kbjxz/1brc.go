package main

import (
	"bytes"
	"os"

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

type fileRegion struct {
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

func partitionFile(h *handler, put func(fileRegion)) error {
	var peekBuf [64]byte
	var start int64
	for {
		if start+h.chunkSize >= h.fileSize {
			put(fileRegion{start, h.fileSize})
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
		put(fileRegion{start, end})

		start = end
	}

	return nil
}
