package main

import (
	"testing"
)

func BenchmarkWriterArrow(b *testing.B) {
	for n := 0; n < b.N; n++ {
		writeParquetArrow()
	}
}
