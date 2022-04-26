package main

import (
	"testing"
)

func BenchmarkReaderArrow(b *testing.B) {
	for n := 0; n < b.N; n++ {
		readParquetArrow()
	}
}
