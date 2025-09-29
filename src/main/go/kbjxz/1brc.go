package main

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

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
	beg, end int64
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
	var beg int64
	for {
		start := time.Now()

		if beg+h.chunkSize >= h.fileSize {
			put <- fileSlice{beg, h.fileSize}
			break
		}

		// find last line break in possible chunk
		var peekOffset = beg + h.chunkSize - int64(len(peekBuf))
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

		*latencies = append(*latencies, time.Since(start))

		put <- fileSlice{beg, end}

		beg = end
	}

	return nil
}

func readFileSlice(
	ctx context.Context, h *handler, latencies *[]time.Duration,
	put chan<- []byte, get <-chan fileSlice,
) error {
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

		start := time.Now()
		gotStart, err := f.Seek(fs.beg, 0)
		if err != nil {
			return errors.Wrapf(err, "seek failed at %+v", fs)
		}
		assert(gotStart == fs.beg, "[seek] exp: %d, got: %d", fs.beg, gotStart)

		buf := <-h.arena
		size := fs.end - fs.beg
		n, err := f.Read(buf[:size])
		if err != nil {
			return errors.Wrapf(err, "[read] failed at %+v", fs)
		}
		assert(int64(n) == size, "[read.n] exp: %d, got: %d, at %+v",
			size, n, fs)

		*latencies = append(*latencies, time.Since(start))

		put <- buf[:n]
	}
}

func parseChunk(
	ctx context.Context, h *handler, latencies *[]time.Duration, get <-chan []byte,
) error {
	var buf []byte
	var ok bool
	var intTemperature [8]byte // sign(<=1)+int(<=3)+dot(=1)+faction(1)
	var result = partialResult{
		Index:    map[string]int{},
		Stations: []stationData{},
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case buf, ok = <-get:
			if !ok {
				return nil
			}
		}

		var start = time.Now()

		var beg int = 0
		for beg < len(buf) {

			// scan city
			stationEnd := bytes.IndexByte(buf[beg:], ';')
			assert(stationEnd != -1, "field delimiter not found: %s", buf[:min(len(buf), 32)])
			station := unsafe.String(&buf[beg], stationEnd-beg)

			// scan temperature
			var tempBuf = intTemperature[:0]
			var iTempSrc = stationEnd + 1
			for {
				if len(tempBuf) == cap(tempBuf) {
					return errors.Errorf("temperature too long: %s", buf[iTempSrc:min(iTempSrc+16, len(buf))])
				}

				if b := buf[iTempSrc]; b == '\n' {
					beg = iTempSrc + 1
					break
				} else if b == '.' {
					iTempSrc++
				} else {
					tempBuf = append(tempBuf, buf[iTempSrc])
					iTempSrc++
				}
			}
			temp, err := strconv.Atoi(unsafe.String(&tempBuf[0], len(tempBuf)))
			if err != nil {
				return errors.Errorf("invalid temperature: %s", tempBuf)
			}

			// insert
			i, ok := result.Index[station]
			if !ok {
				result.Stations = append(result.Stations, stationData{Station: strings.Clone(station)})
				i = len(result.Stations) - 1
				result.Index[station] = i
			}

			s := &result.Stations[i]
			s.Avg = (s.Avg*s.Count + temp) / (s.Count + 1)
			s.Count++
			s.Min = min(s.Min, temp)
			s.Max = max(s.Max, temp)
		}

		*latencies = append(*latencies, time.Since(start))

		h.arena <- buf
	}
}

type parseResult struct {
	station     string
	temperature int
	advacend    int
}

func parseLine2(data []byte) (parseResult, error) {
	var tempBuf [8]byte
	var ret parseResult

	// scan city
	idxData := bytes.IndexByte(data, ';')
	assert(idxData != -1, "field delimiter not found: %s", data[:min(len(data), 32)])
	ret.station = unsafe.String(&data[0], idxData)

	// scan temperature
	idxData++
	var idxTemp = 0
	for idxData < len(data) {
		if idxTemp == cap(tempBuf) {
			beg := len(ret.station) + 0
			end := min(len(data), beg+31)
			return ret, errors.Errorf("temperature too long: %s", data[beg:end])
		}

		if b := data[idxData]; b == '\n' {
			idxData++
			break
		} else if b == '.' {
			idxData++
		} else {
			tempBuf[idxTemp] = b
			idxTemp++
			idxData++
		}
	}

	var err error
	ret.temperature, err = strconv.Atoi(unsafe.String(&tempBuf[0], idxTemp))
	if err != nil {
		return ret, errors.Errorf("invalid temperature: %s", tempBuf)
	}

	ret.advacend = idxData

	return ret, nil
}
