package main

import (
	"github.com/fraugster/parquet-go/floor"
	"github.com/seaguest/log"
)

type record_fraugster struct {
	Format   string `parquet:"format"`
	DataType int32  `parquet:"data_type"`
	Country  string `parquet:"country"`
}

func readParquetFraugster() {
	parquetFilename := "../output_fraugster.parquet"

	fr, err := floor.NewFileReader(parquetFilename)
	if err != nil {
		log.Fatalf("Opening parquet file for writing failed: %v", err)
	}

	var result []record_fraugster
	count := 0

	for fr.Next() {
		var record record_fraugster

		// require.Error(t, hlReader.Scan(int(1)), "%d. Scan into int unexpectedly succeeded", count)
		// require.Error(t, hlReader.Scan(new(int)), "%d. Scan into *int unexpectedly succeeded", count)

		// require.NoError(t, hlReader.Scan(&msg), "%d. Scan failed", count)
		// t.Logf("%d. data = %#v", count, hlReader.data)
		fr.Scan(&record)

		result = append(result, record)
		count++
	}

	if err := fr.Close(); err != nil {
		log.Fatalf("Closing parquet reader failed: %v", err)
	}

}
