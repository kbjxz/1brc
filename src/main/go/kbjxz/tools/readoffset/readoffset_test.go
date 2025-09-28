package readoffset

import (
	"os"
	"testing"
)

func Test_main(t *testing.T) {
	os.Args = []string{
		"",
		"/data00/home/kongbiao/src/1brc/src/main/go/kbjxz/measurements.txt",
		"2718826199",
	}
	main()
}
