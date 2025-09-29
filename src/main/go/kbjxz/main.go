package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
	"unsafe"

	"github.com/pkg/errors"
)

type fileMeta struct {
	FileName    string
	FileSize    int64
	Procs       int
	ChunkSize   int64
	TotalChunks int64
}

func main() {
	// params := must(parseArgs(os.Args))

	// meta := must(getMeta(&params))

	// // split file
	// fileReaders, ctx := errgroup.WithContext(context.Background())
	// partialLists := make([][]stationData, len(meta.Regions))
	// for i := range meta.Regions {
	// 	i := i
	// 	result := &partialLists[i]
	// 	fileReaders.Go(func() (err error) {
	// 		*result, err = _parseRegion(&meta, i)
	// 		return err
	// 	})
	// }
	// if err := fileReaders.Wait(); err != nil {
	// 	fmt.Printf("%+v", context.Cause(ctx))
	// 	return
	// }

	// output(&meta, reduceFinalResult(partialLists))
}

type params struct {
	fileName  string
	procs     int
	chunkSize int64
}

func parseArgs(args []string) (params, error) {
	p := params{
		procs:     1,
		chunkSize: 1024 * MB,
	}
	if len(args) < 2 {
		return p, errors.New("missing filename")
	}

	parsers := []func(string, *params) error{
		func(string, *params) error { return nil },
		func(v string, p *params) error {
			p.fileName = v
			return nil
		},
		func(v string, p *params) error {
			procs, err := strconv.Atoi(args[2])
			if err != nil {
				return errors.WithStack(err)
			}
			p.procs = procs
			return nil
		},
		func(v string, p *params) error {
			chunkSize, err := strconv.Atoi(args[3])
			if err != nil {
				return errors.WithStack(err)
			}
			p.chunkSize = int64(chunkSize)
			return nil
		},
	}

	for i, v := range args {
		err := parsers[i](v, &p)
		if err != nil {
			return p, err
		}
	}
	return p, nil
}

func getMeta(p *params) (fileMeta, error) {
	f := must(os.Open(p.fileName))
	defer f.Close()

	var ret fileMeta
	ret.FileName = p.fileName
	ret.FileSize = (must(f.Stat()).Size())
	ret.Procs = p.procs
	ret.ChunkSize = p.chunkSize
	ret.TotalChunks = ret.FileSize / ret.ChunkSize
	if ret.FileSize%ret.ChunkSize > 0 {
		ret.TotalChunks++
	}

	return ret, nil
}

type stationData struct {
	Station       string
	Count         int
	Min, Max, Avg int
}

type record struct {
	Station string
	Temp    int
}

type latenciesKey struct{}

// parseLine input: %s;%d.%1d
func parseLine(line []byte, buf [8]byte) (record, error) {
	sep := bytes.IndexByte(line, ';')

	lenFloat := len(line) - sep - 1
	assert(
		lenFloat <= len(buf),
		"float point too long: %s", unsafe.String(&line[sep+1], lenFloat))

	iBuf := 0
	iTemp := sep + 1
	for iTemp < len(line) {
		b := line[iTemp]
		iTemp++
		if b == '.' {
			continue
		}
		buf[iBuf] = b
		iBuf++
	}
	temp, err := strconv.Atoi(unsafe.String(&buf[0], iBuf))
	if err != nil {
		return record{}, errors.Wrap(err, unsafe.String(&line[0], len(line)))
	}

	ret := record{
		Station: unsafe.String(&line[0], sep),
		Temp:    temp,
	}
	return ret, nil
}

type partialResult struct {
	Index map[string]int
	List  []stationData
}

func (pr *partialResult) insert(r *record) {
	i, ok := pr.Index[r.Station]
	if !ok {
		pr.List = append(pr.List, stationData{Station: r.Station})
		i = len(pr.List) - 1
		pr.Index[r.Station] = i
	}

	data := &pr.List[i]
	data.Avg = (data.Avg*data.Count + r.Temp) / (data.Count + 1)
	data.Count++
	data.Min = min(data.Min, r.Temp)
	data.Max = max(data.Max, r.Temp)
}

func scanLine(buf []byte) (line []byte, rest []byte) {
	i := bytes.IndexByte(buf, '\n')
	if i == -1 {
		return buf, nil
	}
	return buf[:i], buf[i+1:]
}

func (sd *stationData) String() string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", sd.Station,
		float32(sd.Min)/10.0, float32(sd.Avg)/10.0, float32(sd.Max)/10.0)
}

func reduceFinalResult(partialLists [][]stationData) []stationData {
	var totalLen int
	for _, list := range partialLists {
		totalLen += len(list)
	}
	avgLen := totalLen / len(partialLists)
	resultIndex := make(map[string]int, avgLen)
	result := make([]stationData, 0, avgLen)
	for _, partialList := range partialLists {
		for i := range partialList {
			partialData := &partialList[i]
			idx, ok := resultIndex[partialData.Station]
			if !ok {
				// push partialData as initial value
				result = append(result, *partialData)
				resultIndex[partialData.Station] = len(result) - 1
			} else {
				// reduce
				r := &result[idx]
				r.Min = min(r.Min, partialData.Min)
				r.Avg = (r.Count*r.Avg + partialData.Avg) / (r.Count + 1)
				r.Count += 1
				r.Max = max(r.Max, partialData.Max)
			}
		}
	}
	return result
}

func output(meta *fileMeta, result []stationData) {
	outname := meta.FileName + ".result"
	f := must(os.Create(outname))
	defer f.Close()
	for i := range result {
		f.WriteString(result[i].String())
		f.WriteString("\n")
	}
}

func readFileChunks(ctx context.Context, meta *fileMeta, put chan<- []byte, get <-chan []byte) error {
	f := must(os.Open(meta.FileName))
	defer f.Close()

	var (
		read     = int64(0)
		leftover = 0
		buf      = <-get
	)

	latencies := make([]time.Duration, 0, meta.TotalChunks)
	defer func() {
		if d, ok := ctx.Value(latenciesKey{}).(*[]time.Duration); ok {
			*d = latencies
		}
	}()

	for read < meta.FileSize {
		assert(len(buf) == GB, "[len(buf)] exp: %d, got: %d", GB, len(buf))

		beg := time.Now()

		n, err := f.Read(buf[leftover:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.Wrapf(err, "[readRegion@%d]", read)
		}

		lastLine := bytes.LastIndexByte(buf, '\n')

		if lastLine != -1 {
			// seek last '\n' and append leftover bytes into next buf
			chunkEnd := lastLine + 1
			nextBuf := <-get
			copy(nextBuf, buf[chunkEnd:])
			leftover = len(buf) - chunkEnd
			put <- buf[:chunkEnd]
			buf = nextBuf
		} else {
			put <- buf
			buf = <-get
			leftover = 0
		}

		latencies = append(latencies, time.Since(beg))

		read += int64(n)

		select {
		case <-ctx.Done():
			return nil
		default:
			continue
		}
	}
	assert(read == meta.FileSize,
		"[readRegion] exp: %d, got: %d", meta.FileSize, read)
	return nil
}

func parseChunks(ctx context.Context, put chan<- []byte, get <-chan []byte) ([]stationData, error) {
	var (
		line    []byte
		lineBuf [8]byte
		partial = partialResult{
			Index: map[string]int{},
			List:  make([]stationData, 0),
		}
		chunk []byte
		ok    bool
	)

	latencies := []time.Duration{}
	defer func() {
		if d, ok := ctx.Value(latenciesKey{}).(*[]time.Duration); ok {
			*d = latencies
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case chunk, ok = <-get:
		}

		if !ok {
			break
		}

		beg := time.Now()

		remain := chunk
		for len(remain) != 0 {
			line, remain = scanLine(remain)
			if len(line) == 0 {
				continue
			}

			record, err := parseLine(line, lineBuf)
			if err != nil {
				return nil, errors.WithMessage(err, fmt.Sprintf("line:%s", string(line)))
			}

			partial.insert(&record)
		}
		put <- resetChunk(chunk)

		latencies = append(latencies, time.Since(beg))
	}

	ret := partial.List
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Station < ret[j].Station
	})

	return ret, nil
}

func resetChunk(b []byte) []byte {
	return unsafe.Slice(unsafe.SliceData(b), cap(b))
}
