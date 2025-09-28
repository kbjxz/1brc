package readoffset

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		panic("invalid number of args")
	}

	fname := os.Args[1]
	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	offsetStr := os.Args[2]
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		panic(err)
	}

	const contextBytes = 64
	start := int64(max(offset-contextBytes, 0))
	_, err = f.Seek(start, 0)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, ((offset + contextBytes) - start))
	n, err := f.Read(buf[:])
	if err != nil {
		panic(err)
	}

	fmt.Println("[strings.Split]")
	for i, line := range strings.Split(string(buf), "\n") {
		fmt.Println(i, ":", line)
	}

	fmt.Println("\n[strings.IndexRune]")
	var bufStr = string(buf)
	for i := 0; ; i++ {
		idx := strings.IndexRune(bufStr, '\n')
		if idx == -1 {
			fmt.Println(i, ":", bufStr)
			break
		}
		fmt.Println(i, ":", bufStr[:idx])
		bufStr = bufStr[idx+1:]
	}

	fmt.Println("\n[bytes.IndexByte]")
	for i, b := 0, buf[:n]; len(b) > 0; i++ {
		idx := bytes.IndexByte(b, '\n')
		if idx == -1 {
			fmt.Println(i, ":", string(b))
			break
		}

		fmt.Println(i, ":", string(b[:idx]))
		b = b[idx+1:]
	}
}
