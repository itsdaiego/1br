package main

import (
	"bufio"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Result struct {
	text string
}

const CHUNK_SIZE = 1024 * 1024

type StationData struct {
	temperatures []float64
	mu           sync.Mutex
}

var STATIONS_MAP []map[string]*StationData
var stationLocks []sync.Mutex
var numShards = 128

func getShard(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key))) % numShards
}

func worker(ctx context.Context, wg *sync.WaitGroup, fileReadJobs <-chan string, results chan<- Result) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Exiting worker")
			return
		case job, ok := <-fileReadJobs:
			if !ok {
				return
			}
			results <- Result{job}
		}
	}
}

func calculateMinMaxAvg(values []float64) (float64, float64, float64) {
	if len(values) == 0 {
		return 0, 0, 0
	}

	min := values[0]
	max := values[0]
	total := 0.0

	for _, temp := range values {
		if temp < min {
			min = temp
		}
		if temp > max {
			max = temp
		}
		total += temp
	}

	avg := total / float64(len(values))
	return min, max, avg
}

func parseValues(chunk string) {
	lines := strings.Split(chunk, "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Split(line, ";")

		if len(parts) != 2 {
			continue
		}

		station := strings.TrimSpace(parts[0])
		temp := strings.TrimSpace(parts[1])

		if temp == "" {
			continue
		}

		temperature, err := strconv.ParseFloat(temp, 64)
		if err != nil {
			fmt.Println("Error converting to float: ", err)
			continue
		}

		shard := getShard(station)
		stationLocks[shard].Lock()

		if _, ok := STATIONS_MAP[shard][station]; !ok {
			STATIONS_MAP[shard][station] = &StationData{}
		}

		data := STATIONS_MAP[shard][station]
		stationLocks[shard].Unlock()

		data.mu.Lock()

		data.temperatures = append(data.temperatures, temperature)

		data.mu.Unlock()
	}
}

func readFile(fileReadJobs chan<- string) error {
	file, err := os.Open("./measurements.txt")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var buffer strings.Builder

	for {
		part, isPrefix, err := reader.ReadLine()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		buffer.Write(part)
		if !isPrefix {
			fileReadJobs <- buffer.String()
			buffer.Reset()
		}
	}

	close(fileReadJobs)
	return nil
}

func main() {
	start := time.Now()

	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)

	var wg sync.WaitGroup
	var resultWg sync.WaitGroup

	goroutineCount := numCpu * 2

	results := make(chan Result, goroutineCount)
	fileReadJobs := make(chan string, goroutineCount)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	STATIONS_MAP = make([]map[string]*StationData, numShards)
	stationLocks = make([]sync.Mutex, numShards)
	for i := 0; i < numShards; i++ {
		STATIONS_MAP[i] = make(map[string]*StationData)
	}

	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, fileReadJobs, results)
	}

	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for result := range results {
			parseValues(result.text)
		}
	}()

	go func() {
		err := readFile(fileReadJobs)
		if err != nil {
			fmt.Println("Error: ", err)
			cancel()
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	resultWg.Wait()

	file, err := os.Create("./output.txt")

	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	var contents []string

	for shard := 0; shard < numShards; shard++ {
		for station, data := range STATIONS_MAP[shard] {
			data.mu.Lock()
			min, max, avg := calculateMinMaxAvg(data.temperatures)
			data.mu.Unlock()
			contents = append(contents, fmt.Sprintf("%s;%.2f;%.2f;%.2f", station, min, max, avg))
		}
	}

	sort.Strings(contents)

	for _, line := range contents {
		writer.WriteString(line + "\n")

	}
	writer.Flush()

	elapsed := time.Since(start)
	fmt.Printf("Time elapsed: %s\n", elapsed)
	fmt.Printf("Number of stations: %d\n", len(contents))
}
