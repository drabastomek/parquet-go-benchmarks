package main

import (
	"testing"
)

func BenchmarkWriterSegmentio(b *testing.B) {
	for n := 0; n < b.N; n++ {
		writeParquetSegmentio()
	}
}
