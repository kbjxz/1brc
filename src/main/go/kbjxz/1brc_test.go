package main

import "testing"

func Test_partitionFile(t *testing.T) {
	h, err := newHandler("./measurements.txt", 250*MB, 1, 1)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var regions []fileRegion
	err = partitionFile(&h, func(r fileRegion) {
		regions = append(regions, r)
	})
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for i, r := range regions {
		t.Logf("%d: {start:%d, end:%d, size:%d}\n", i, r.start, r.end, r.end-r.start)
	}
}
