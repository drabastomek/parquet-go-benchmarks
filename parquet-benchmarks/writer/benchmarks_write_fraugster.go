package main

import (
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/seaguest/log"
)

type record_fraugster struct {
	Format   string `parquet:"format"`
	DataType int32  `parquet:"data_type"`
	Country  string `parquet:"country"`
}

func writeParquetFraugster() {
	schemaDef, err := parquetschema.ParseSchemaDefinition(
		`message test {
			required binary format (STRING);
			required int32 data_type;
			required binary country (STRING);
		}`)
	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}

	parquetFilename := "../output_fraugster.parquet"

	fw, err := floor.NewFileWriter(parquetFilename,
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		log.Fatalf("Opening parquet file for writing failed: %v", err)
	}

	num := 10000
	for i := 0; i < num; i++ {
		stu := record_fraugster{
			Format:   "Test",
			DataType: 1,
			Country:  "US",
		}
		if err = fw.Write(stu); err != nil {
			log.Error("Write error", err)
		}
	}

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet writer failed: %v", err)
	}

}
