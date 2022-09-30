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

func readParquetSegmentio() {
	file, err := os.Open("../output_segmentio.parquet")
	if err != nil {
		return
	}
	defer file.Close()

	reader := parquet.NewGenericReader[record_segmentio](file)
	buffer := make([]record_segmentio, 1000)

	for {
		_, err := reader.Read(buffer)
		if err != nil {
			return
		}
	}

	// // Closing the reader is necessary to flush buffers and write the file footer.
	// if err := file.Close(); err != nil {
	// 	return
	// }
}
