package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"unsafe"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type fileMeta struct {
	FileName  string
	FileSize  int64
	Procs     int
	ChunkSize int64
	BufSize   int
	Regions   []fileRegion
}

type fileRegion struct {
	Start, End int64 // [start, end)
}

func main() {
	assert(len(os.Args) > 1, "missing filename")
	fname := os.Args[1]
	meta := must(getMeta(fname))

	// split file
	fileReaders, ctx := errgroup.WithContext(context.Background())
	partialLists := make([][]stationData, len(meta.Regions))
	for i := range meta.Regions {
		region := meta.Regions[i]
		result := &partialLists[i]
		fileReaders.Go(func() (err error) {
			*result, err = parseRegion(&meta, region)
			return err
		})
	}
	if err := fileReaders.Wait(); err != nil {
		fmt.Printf("%+v", context.Cause(ctx))
		return
	}

	output(&meta, reduceFinalResult(partialLists))
}

func getMeta(fname string) (fileMeta, error) {
	f := must(os.Open(fname))
	defer f.Close()

	var ret fileMeta
	ret.FileName = fname
	ret.FileSize = (must(f.Stat()).Size())
	ret.Procs = runtime.GOMAXPROCS(0) // expect filesize > GB
	ret.ChunkSize = ret.FileSize / int64(ret.Procs)
	const defaultBufSize = 4 * 1024 * 1024
	ret.BufSize = int(min(ret.ChunkSize, defaultBufSize))

	ret.Regions = make([]fileRegion, ret.Procs)
	offset := int64(0)
	peekBuf := [128]byte{}
	for i := 0; i < len(ret.Regions); i++ {
		region := &ret.Regions[i]
		region.Start = offset

		if i == len(ret.Regions)-1 {
			region.End = ret.FileSize
			break
		}

		// search '\n' in [offset-1, (offset-1)+128)
		offset += ret.ChunkSize
		must(f.Seek(max(0, offset-1), 0))
		n, err := f.Read(peekBuf[:])
		if err != nil {
			return ret, errors.WithStack(err)
		}
		lineBreak := bytes.IndexByte(peekBuf[:n], '\n')

		// include '\n' in the region for easy scanline
		assert(lineBreak != -1, "'\n' not found")
		// fmt.Printf("[region:%d] tail: %s\n", i, strings.ReplaceAll(
		// 	string(peekBuf[:lineBreak+1]), "\n", "\\n"))
		offset += int64(lineBreak + 1)
		region.End = offset
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

// parseLine input: %s;%d.%1d
func parseLine(line []byte, buf [8]byte) (record, error) {
	sep := bytes.IndexByte(line, ';')

	lenFloat := len(line) - sep - 1
	assert(
		lenFloat <= len(buf),
		"float point too long:"+unsafe.String(&line[sep+1], lenFloat))

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

func parseRegion(meta *fileMeta, region fileRegion) ([]stationData, error) {
	f := must(os.Open(meta.FileName))
	defer f.Close()
	must(f.Seek(region.Start, 0))

	regionSize := region.End - region.Start
	buf := make([]byte, regionSize)
	readSize, err := f.Read(buf)
	if err != nil {
		return nil, errors.Wrapf(err, "region:%+v", region)
	}
	assert(int64(readSize) == regionSize,
		fmt.Sprintf("[readRegion] exp:%d, got: %d", readSize, regionSize))

	const expSize = 50000
	var (
		line    []byte
		partial = partialResult{
			Index: make(map[string]int, expSize),
			List:  make([]stationData, expSize),
		}
		lineBuf [8]byte
		offset  = region.Start
	)
	for len(buf) != 0 {
		line, buf = scanLine(buf)
		record, err := parseLine(line, lineBuf)
		if err != nil {
			return nil, errors.WithMessage(err, fmt.Sprintf(
				"line:%s, file_offset:%d", string(line), offset))
		}
		partial.insert(&record)
		offset += int64(len(line))
	}

	ret := partial.List
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Station < ret[j].Station
	})

	return ret, nil
}

func scanLine(buf []byte) (line []byte, rest []byte) {
	i := bytes.IndexByte(buf, '\n')
	if i == -1 {
		return buf, nil
	}
	return buf[:i], buf[min(len(buf), i+1):]
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
