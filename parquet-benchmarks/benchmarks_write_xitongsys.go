package main

import (
	"os"

	"github.com/seaguest/log"
	parquetXitongsys "github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type record_xitongsys struct {
	Format   string `parquet:"name=format, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DataType int32  `parquet:"name=data_type, type=INT32, encoding=PLAIN"`
	Country  string `parquet:"name=country, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func writeParquetXitongsys() {
	var err error
	w, err := os.Create("output_xitongsys.parquet")
	if err != nil {
		log.Error("Can't create local file", err)
		return
	}

	//write
	pw, err := writer.NewParquetWriterFromWriter(w, new(record_xitongsys), 4)
	if err != nil {
		log.Error("Can't create parquet writer", err)
		return
	}

	pw.CompressionType = parquetXitongsys.CompressionCodec_SNAPPY
	num := 10000
	for i := 0; i < num; i++ {
		stu := record_xitongsys{
			Format:   "Test",
			DataType: 1,
			Country:  "US",
		}
		if err = pw.Write(stu); err != nil {
			log.Error("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Error("WriteStop error", err)
		return
	}
	w.Close()
}
