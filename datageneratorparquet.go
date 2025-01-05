package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/xitongsys/parquet-go/writer"
	"github.com/xitongsys/parquet-go-source/local"
)

type TickData struct {
	Timestamp string  `parquet:"name=timestamp, type=UTF8, encoding=PLAIN"`
	Symbol    string  `parquet:"name=symbol, type=UTF8, encoding=PLAIN"`
	Price     float64 `parquet:"name=price, type=DOUBLE"`
	Volume    int64   `parquet:"name=volume, type=INT64"`
}

func main() {
	// File to write tick data
	filename := "tick_data.parquet"
	file, err := local.NewLocalFileWriter(filename)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer file.Close()

	pw, err := writer.NewParquetWriter(file, new(TickData), 4) // 4 is the number of parallel writers
	if err != nil {
		fmt.Printf("Error creating parquet writer: %v\n", err)
		return
	}
	defer pw.WriteStop()

	symbols := []string{"AAPL", "GOOG", "MSFT", "AMZN", "TSLA"}
	numRows := 5_000_000_000
	batchSize := 100_000 // Write rows in batches to reduce memory usage

	rand.Seed(time.Now().UnixNano())
	fmt.Printf("Generating %d rows of tick data...\n", numRows)

	for i := 0; i < numRows; i += batchSize {
		for j := 0; j < batchSize && i+j < numRows; j++ {
			timestamp := time.Now().Add(time.Duration(-rand.Int63n(1_000_000)) * time.Millisecond).Format(time.RFC3339Nano)
			symbol := symbols[rand.Intn(len(symbols))] // Randomly select a symbol
			price := 100 + rand.Float64()*50          // Random price between 100 and 150
			volume := rand.Int63n(1000) + 1           // Random volume between 1 and 1000

			data := TickData{
				Timestamp: timestamp,
				Symbol:    symbol,
				Price:     price,
				Volume:    volume,
			}

			if err := pw.Write(data); err != nil {
				fmt.Printf("Error writing data: %v\n", err)
				return
			}
		}

		fmt.Printf("Written %d/%d rows...\n", i+batchSize, numRows)
	}

	fmt.Println("Data generation complete.")
}

