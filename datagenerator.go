package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	// File to write tick data
	filename := "tick_data.csv"
	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	err = writer.Write([]string{"timestamp", "symbol", "price", "volume"})
	if err != nil {
		fmt.Printf("Error writing header: %v\n", err)
		return
	}

	symbols := []string{"AAPL", "GOOG", "MSFT", "AMZN", "TSLA"}
	numRows := 5_000_000_000
	batchSize := 100_000 // Write rows in batches to reduce memory usage

	rand.Seed(time.Now().UnixNano())
	fmt.Printf("Generating %d rows of tick data...\n", numRows)

	for i := 0; i < numRows; i += batchSize {
		var batch [][]string
		for j := 0; j < batchSize && i+j < numRows; j++ {
			timestamp := time.Now().Add(time.Duration(-rand.Int63n(1_000_000)) * time.Millisecond).Format(time.RFC3339Nano)
			symbol := symbols[rand.Intn(len(symbols))] // Randomly select a symbol
			price := fmt.Sprintf("%.2f", 100+rand.Float64()*50) // Random price between 100 and 150
			volume := strconv.FormatInt(rand.Int63n(1000)+1, 10) // Random volume between 1 and 1000

			batch = append(batch, []string{
				timestamp, symbol, price, volume,
			})
		}

		if err := writer.WriteAll(batch); err != nil {
			fmt.Printf("Error writing batch: %v\n", err)
			return
		}

		fmt.Printf("Written %d/%d rows...\n", i+batchSize, numRows)
	}

	fmt.Println("Data generation complete.")
}

