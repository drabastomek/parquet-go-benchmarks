package main

import (
	"testing"
)

func BenchmarkReaderFraugster(b *testing.B) {
	for n := 0; n < b.N; n++ {
		readParquetFraugster()
	}
}
