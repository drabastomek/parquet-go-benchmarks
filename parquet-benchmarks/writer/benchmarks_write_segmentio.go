package main

import (
	"os"

	"github.com/segmentio/parquet-go"
)

type record_segmentio struct {
	Format   string `parquet:"name=format"`
	DataType int32  `parquet:"name=data_type"`
	Country  string `parquet:"name=country"`
}

func writeParquetSegmentio() {
	file, err := os.Create("../output_segmentio.parquet")
	if err != nil {
		return
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[record_segmentio](file)
	num := 10000
	rec := make([]record_segmentio, num)

	for i := range rec {
		rec[i] = record_segmentio{
			Format:   "Test",
			DataType: 1,
			Country:  "US",
		}
	}

	if _, err := writer.Write(rec); err != nil {
		return
	}

	// Closing the writer is necessary to flush buffers and write the file footer.
	if err := writer.Close(); err != nil {
		return
	}
}
