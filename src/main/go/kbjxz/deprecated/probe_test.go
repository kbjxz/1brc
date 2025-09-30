package main

import (
	"encoding/csv"
	"hash/maphash"
	. "kbjxz/util"
	"os"
	"sort"
	"testing"
)

var seed = maphash.MakeSeed()

func stationList() []string {
	f := Must(os.Open("weather_stations.csv"))
	defer f.Close()

	csvr := csv.NewReader(f)
	data := Must(csvr.ReadAll())
	ret := make([]string, len(data))
	for i, record := range data {
		ret[i] = record[0]
	}
	return ret
}

type probeRecord struct {
	probes int
	exist  bool
}

func probe(stations []string, size int) []probeRecord {
	table := make([]probeRecord, size)
	for _, v := range stations {
		h := int(maphash.String(seed, v) % uint64(size))
		probes := 0
		for ; table[h%size].exist; h++ {
			probes++
		}
		table[h%size] = probeRecord{probes, true}
	}

	sort.Slice(table, func(i, j int) bool {
		return table[i].probes < table[j].probes
	})
	return table
}

func Test_probe(t *testing.T) {
	list := stationList()
	for _, size := range []int{
		50423, 100003, 300017,
	} {
		ret := probe(list, size)
		j := sort.Search(size, func(i int) bool {
			return ret[i].probes > 0
		})
		t.Logf("%f%%=%d/%d needs probing", float64(j)/float64(len(ret)), j, len(ret))
		t.Log("min:", ret[j], "med:", ret[(j+len(ret))/2], "max", ret[len(ret)-1])
	}
}
