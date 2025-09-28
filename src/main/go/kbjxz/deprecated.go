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
