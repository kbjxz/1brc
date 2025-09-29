package main

import (
	"bytes"
	"fmt"
)

type _record struct {
	Station string
	Temp    float32
}

func _parseLine(line []byte) (_record, error) {
	sep := bytes.IndexByte(line, ';')
	assert(sep != -1, "invalid record")
	ret := _record{
		Station: string(line[:sep]),
	}
	must(fmt.Sscanf(string(line[sep+1:]), "%f", &ret.Temp))
	return ret, nil
}

type _partialResult map[string]stationData

func (pr _partialResult) insert(r *record) {
	data, _ := pr[r.Station]
	data.Avg = (data.Avg*data.Count + r.Temp) / (data.Count + 1)
	data.Count++
	data.Min = min(data.Min, r.Temp)
	data.Max = max(data.Max, r.Temp)
	pr[r.Station] = data
}

func _reduceFinalResult(partialLists [][]stationData) []stationData {
	var totalLen int
	for _, list := range partialLists {
		totalLen += len(list)
	}
	avgLen := totalLen / len(partialLists)
	resultIndex := make(map[string]int, avgLen)
	result := make([]stationData, 0, avgLen)
	end := len(partialLists)
	for end > 0 {
		for i := 0; i < end; i++ {
			// remove exhausted list
			if len(partialLists[i]) == 0 {
				end--
				partialLists[i], partialLists[end] = partialLists[end], partialLists[i]
				continue
			}

			partialData := &partialLists[i][0]
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

			partialLists[i] = partialLists[i][1:]
		}
	}
	return result
}

// func _parseRegion(meta *fileMeta, i int) ([]stationData, error) {
// 	region := &meta.Regions[i]
// 	f := must(os.Open(meta.FileName))
// 	defer f.Close()
// 	must(f.Seek(region.Start, 0))

// 	regionSize := region.End - region.Start
// 	buf := make([]byte, regionSize)
// 	readSize, err := f.Read(buf)
// 	if err != nil {
// 		return nil, errors.Wrapf(err, "region:%+v", region)
// 	}
// 	assert(int64(readSize) == regionSize,
// 		"[readRegion:%d] exp:%d, got: %d", i, regionSize, readSize)

// 	const expSize = 50000
// 	var (
// 		line    []byte
// 		partial = partialResult{
// 			Index: make(map[string]int, expSize),
// 			List:  make([]stationData, expSize),
// 		}
// 		lineBuf [8]byte
// 		offset  = region.Start
// 	)
// 	for len(buf) != 0 {
// 		line, buf = scanLine(buf)
// 		record, err := parseLine(line, lineBuf)
// 		if err != nil {
// 			return nil, errors.WithMessage(err, fmt.Sprintf(
// 				"line:%s, file_offset:%d", string(line), offset))
// 		}
// 		partial.insert(&record)
// 		offset += int64(len(line))
// 	}

// 	ret := partial.List
// 	sort.Slice(ret, func(i, j int) bool {
// 		return ret[i].Station < ret[j].Station
// 	})

// 	return ret, nil
// }