package main

import (
	"testing"
)

func BenchmarkReaderSegmentio(b *testing.B) {
	for n := 0; n < b.N; n++ {
		readParquetSegmentio()
	}
}
