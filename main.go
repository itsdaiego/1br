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
  min, max, sum float64
  count         int
}

var STATIONS_MAP []sync.Map
var numShards = 128

func getShard(key string) int {
  return int(crc32.ChecksumIEEE([]byte(key))) % numShards
}

func worker(ctx context.Context, wg *sync.WaitGroup, fileReadJobs <-chan []byte, results chan<- Result) {
  defer wg.Done()

  for {
    select {
    case <-ctx.Done():
      return
    case chunk, ok := <-fileReadJobs:
      if !ok {
        return
      }
      results <- Result{string(chunk)}
    }
  }
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

    station := parts[0]
    temp := parts[1]

    if temp == "" {
      continue
    }

    temperature, err := strconv.ParseFloat(temp, 64)
    if err != nil {
      continue
    }

    shard := getShard(station)
    data, _ := STATIONS_MAP[shard].LoadOrStore(station, &StationData{min: temperature, max: temperature})
    stationData := data.(*StationData)

    if temperature < stationData.min {
      stationData.min = temperature
    }
    if temperature > stationData.max {
      stationData.max = temperature
    }
    stationData.sum += temperature
    stationData.count++
  }
}

func readFile(fileReadJobs chan<- []byte) error {
  file, err := os.Open("./measurements.txt")
  if err != nil {
    return err
  }
  defer file.Close()

  reader := bufio.NewReader(file)
  chunk := make([]byte, CHUNK_SIZE)

  for {
    n, err := reader.Read(chunk)
    if err != nil {
      if err.Error() == "EOF" {
        break
      }
      return err
    }

    fileReadJobs <- chunk[:n]
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

  goroutineCount := numCpu * 4

  results := make(chan Result, goroutineCount)
  fileReadJobs := make(chan []byte, goroutineCount)

  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()

  STATIONS_MAP = make([]sync.Map, numShards)

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
    STATIONS_MAP[shard].Range(func(key, value interface{}) bool {
      station := key.(string)
      data := value.(*StationData)
      avg := data.sum / float64(data.count)
      contents = append(contents, fmt.Sprintf("%s;%.2f;%.2f;%.2f", station, data.min, data.max, avg))
      return true
    })
  }

  sort.Strings(contents)

  for _, line := range contents {
    writer.WriteString(line + "\n")
  }
  writer.Flush()

  elapsed := time.Since(start)
  fmt.Printf("Time elapsed: %s\n", elapsed)
}
