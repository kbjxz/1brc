package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

type handler struct {
	f          *os.File
	fileName   string
	fileSize   int64
	readProcs  int64
	parseProcs int64
	chunkSize  int64
	arena      chan []byte
	isDebug    bool
}

type fileSlice struct {
	beg, end int64
}

func newHandler(fileName string, chunkSize int64, readProcs, parseProcs int) (handler, error) {
	assert(readProcs > 0, "[readprocs] exp > 0, got: %d", readProcs)
	assert(parseProcs > 0, "[parseprocs] exp > 0, got: %d", parseProcs)

	var ret = handler{
		fileName:   fileName,
		readProcs:  int64(readProcs),
		parseProcs: int64(parseProcs),
		arena:      make(chan []byte, parseProcs+1),
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

type result struct {
	datas          []stationData
	sliceLatencies []time.Duration
	readLatencies  [][]time.Duration
	parseLatencies [][]time.Duration
	mergeLatencies []time.Duration
}

func run(h *handler) (result, error) {
	eg, ctx := errgroup.WithContext(context.Background())

	var latenciesCount = h.fileSize/h.chunkSize + 1
	var sliceLatencies = make([]time.Duration, 0, latenciesCount)
	var fsliceCh = make(chan fileSlice)
	eg.Go(func() error {
		defer close(fsliceCh)
		return sliceFile(h, &sliceLatencies, fsliceCh)
	})

	var readWg sync.WaitGroup
	readWg.Add(int(h.readProcs))
	var parseCh = make(chan []byte)
	var readLatencies = make([][]time.Duration, h.readProcs)
	for i := 0; i < int(h.readProcs); i++ {
		latencies := &readLatencies[i]
		*latencies = make([]time.Duration, 0, latenciesCount)
		eg.Go(func() error {
			defer readWg.Done()
			return readFileSlice(ctx, h, latencies, parseCh, fsliceCh)
		})
	}
	eg.Go(func() error {
		readWg.Wait()
		close(parseCh)
		return nil
	})

	var parseWg sync.WaitGroup
	parseWg.Add(int(h.parseProcs))
	var parseLatencies = make([][]time.Duration, int(h.parseProcs))
	var reduceCh = make(chan []stationData)
	for i := 0; i < int(h.parseProcs); i++ {
		latencies := &parseLatencies[i]
		*latencies = make([]time.Duration, 0, latenciesCount)
		eg.Go(func() error {
			defer parseWg.Done()
			return parseChunk(ctx, h, latencies, reduceCh, parseCh)
		})
	}
	eg.Go(func() error {
		parseWg.Wait()
		close(reduceCh)
		return nil
	})

	var mergeLatencies = make([]time.Duration, 0, latenciesCount)
	var datas []stationData
	eg.Go(func() error {
		datas = reduceStationDatas(ctx, h, &mergeLatencies, reduceCh)
		return nil
	})

	wait := make(chan error)
	go func() {
		defer close(wait)
		wait <- eg.Wait()
	}()
	select {
	case <-ctx.Done():
		err := context.Cause(ctx)
		if !errors.Is(err, context.Canceled) {
			return result{}, err
		}
	case err := <-wait:
		if err != nil {
			return result{}, err
		}
	}

	if !h.isDebug {
		printStationDatas(h, datas)
	}

	return result{
		datas:          datas,
		sliceLatencies: sliceLatencies,
		readLatencies:  readLatencies,
		parseLatencies: parseLatencies,
		mergeLatencies: mergeLatencies,
	}, nil
}

func sliceFile(h *handler, latencies *[]time.Duration, put chan<- fileSlice) error {
	var peekBuf [64]byte
	var beg int64
	for {
		start := time.Now()

		if beg+h.chunkSize >= h.fileSize {
			put <- fileSlice{beg, h.fileSize}
			if h.isDebug {
				fmt.Printf("[sliceFile] %+v\n", fileSlice{beg, h.fileSize})
			}
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
		if h.isDebug {
			fmt.Printf("[sliceFile] %+v\n", fileSlice{beg, h.fileSize})
		}

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

		if h.isDebug {
			fmt.Printf("[readFile] %+v\n", fs)
		}
	}
}

func parseChunk(
	ctx context.Context, h *handler, latencies *[]time.Duration,
	put chan<- []stationData, get <-chan []byte,
) error {
	var buf []byte
	var ok bool
	var result = partialResult{
		Index:    map[string]int{},
		Stations: []stationData{},
	}
	var shouldBreak = false
	for {
		select {
		case <-ctx.Done():
			return nil
		case buf, ok = <-get:
			shouldBreak = !ok
		}

		if shouldBreak {
			break
		}

		var start = time.Now()
		for data := buf; len(data) > 0; {
			pr, err := parseLine2(data)
			if err != nil {
				return err
			}

			// insert
			i, ok := result.Index[pr.station]
			if !ok {
				result.Stations = append(result.Stations, stationData{
					Station: strings.Clone(pr.station),
				})
				i = len(result.Stations) - 1
				result.Index[pr.station] = i
			}

			s := &result.Stations[i]
			s.Avg = (s.Avg*s.Count + pr.temperature) / (s.Count + 1)
			s.Count++
			s.Min = min(s.Min, pr.temperature)
			s.Max = max(s.Max, pr.temperature)

			// advance
			data = data[pr.advacend:]
		}

		*latencies = append(*latencies, time.Since(start))

		if h.isDebug {
			fmt.Printf("[parseChunk] len: %s\n", sprintSize(int64(len(buf))))
		}

		h.arena <- resetChunk(buf)
	}

	sort.Slice(result.Stations, func(i, j int) bool {
		return result.Stations[i].Station < result.Stations[j].Station
	})
	put <- result.Stations
	return nil
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
	for {
		// last line in file has no linebreak
		if idxData == len(data) {
			break
		}

		// bound check
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

	// parse temperature as int
	var err error
	ret.temperature, err = strconv.Atoi(unsafe.String(&tempBuf[0], idxTemp))
	if err != nil {
		return ret, errors.Errorf("invalid temperature: %s", tempBuf)
	}

	ret.advacend = idxData

	return ret, nil
}

func reduceStationDatas(
	ctx context.Context, h *handler, latencies *[]time.Duration, get <-chan []stationData,
) []stationData {
	resultIndex := make(map[string]int)
	result := []stationData{}
	for {
		stationDatas, status := selectReceive(ctx, get)
		if status == canceled {
			return nil
		} else if status == closed {
			break
		}

		var start = time.Now()
		for i := range stationDatas {
			v := &stationDatas[i]
			idx, ok := resultIndex[v.Station]
			if !ok {
				// push partialData as initial value
				result = append(result, *v)
				resultIndex[v.Station] = len(result) - 1
			} else {
				// reduce
				r := &result[idx]
				r.Min = min(r.Min, v.Min)
				r.Avg = (r.Count*r.Avg + v.Avg) / (r.Count + 1)
				r.Count += 1
				r.Max = max(r.Max, v.Max)
			}
		}
		(*latencies) = append((*latencies), time.Since(start))

		if h.isDebug {
			fmt.Printf("[reduce] len: %d\n", len(stationDatas))
		}
	}
	return result
}

type selectRetStatus string

const (
	canceled selectRetStatus = "canceled"
	ready    selectRetStatus = "ready"
	closed   selectRetStatus = "closed"
)

func selectReceive[T any](ctx context.Context, ch <-chan T) (T, selectRetStatus) {
	var v T
	var ok bool
	select {
	case <-ctx.Done():
		return v, canceled
	case v, ok = <-ch:
		if !ok {
			return v, closed
		}
		return v, ready
	}
}

func printStationDatas(h *handler, result []stationData) {
	for _, v := range result {
		fmt.Println(v.String())
	}
}

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
