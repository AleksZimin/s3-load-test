package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	// "github.com/aws/aws-sdk-go-v2/aws/config"

	"github.com/aws/aws-sdk-go-v2/credentials"
)

var (
	readSmallRunning int64 = 0
	readLargeRunning int64 = 0
	errorLogger      *log.Logger
	S3_ENDPOINT      string = "http://10.210.0.67:19000"

	scriptStartTime time.Time
	totalBytesRead  uint64 = 0

	requestCountSmall uint64 = 0
	requestCountLarge uint64 = 0

	failureCountSmall uint64 = 0
	failureCountLarge uint64 = 0
)

const (
	S3_REGION     = "us-east-1"
	S3_BUCKET     = "test-bucket"
	S3_ACCESS_KEY = "minioadmin"
	S3_SECRET_KEY = "minio-strong-secret"

	MODE_AI = "ai"
	MODE_BS = "block"
)

func main() {
	MODES := fmt.Sprintf("(%s|%s)", MODE_AI, MODE_BS)

	mode := flag.String("mode", MODE_AI, fmt.Sprintf("Select mode (%s)", MODES))
	s3Endpoint := flag.String("endpoint-url", S3_ENDPOINT, "S3 endpoint URL")
	workers := flag.Int("workers", 10, "Number of parallel workers")
	smallStart := flag.Int("small-start", 0, "Start of small file range")
	smallEnd := flag.Int("small-end", 0, "End of small file range")
	smallCount := flag.Int("small-count", 1000, "Number of small files to read simultaneously")
	cycles := flag.Int("cycles", -1, "Number of cycles per worker")
	blockSize := flag.Int("block-size-mb", 10, "Large file download block size")
	timeoutSeconds := flag.Int("connection-timeout", 0, "Connection timeout in seconds")
	flag.Parse()

	if *workers <= 0 || *smallStart < 0 || *smallEnd <= 0 || *smallStart >= *smallEnd {
		fmt.Printf(`Parameter(s) error!
		
		usage: -workers=N -mode %s [ModeOptions] [-cycles=N]

		Each worker for <mode> does <cycles> times:
		- ai: 
		  - Read small/file<rand(small-start, small-end)>.txt
		  - <small-count> simultaneous:
		  	- Read small/file<rand(small-start, small-end)>.txt
			- Read block of <block-size-mb> size in random location of large/largefile_<rand(0, 50000)>.bin, assuming large file size 100Mb
		- block:
		  - Read block of <block-size-mb> size in random location of large/largefile_<rand(0, 50000)>.bin, assuming large file size 100Mb
		
		Common options:
		- workers = amount of simultaneously running workers. Must be > 0.
		- cycles = number of cycles per worker (default 0 - infinite)
		- connection-timeout = Connection timeout in seconds. 0 or not set for disabling timeout`, MODES)
		os.Exit(1)
	}

	logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY, 0644)
	//logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("failed to open log file: %v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()

	errorLogger = log.New(logFile, "ERROR: ", log.LstdFlags|log.Lmicroseconds)

	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	// Setup signal handling for Ctrl+C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signalChan:
			fmt.Println("\nReceived shutdown signal. Cancelling context...")
			cancelFunc() // Cancel the context when signal is received
		case <-ctx.Done():
			// Context cancelled by other means
		}
	}()

	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	randSeed := time.Now().UnixNano()
	log.Printf("Rand seed: %v", randSeed)
	rand.Seed(randSeed)

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(S3_REGION),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(S3_ACCESS_KEY, S3_SECRET_KEY, "")),
		config.WithHTTPClient(&http.Client{Timeout: time.Second * time.Duration(*timeoutSeconds)}),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               *s3Endpoint,
					SigningRegion:     S3_REGION,
					HostnameImmutable: true,
				}, nil
			}),
		),
	)

	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
		errorLogger.Printf("Failed to load AWS config: %v", err)
		os.Exit(1)
	}

	s3client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	scriptStartTime = time.Now()
	var wg sync.WaitGroup
	wg.Add(*workers)
	for worker := range *workers {
		go func(worker int) {
			defer wg.Done()

			for cycle := 1; *cycles < 0 || cycle <= *cycles; cycle++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				switch *mode {
				case MODE_AI:
					runAIWorker(ctx, worker, cycle, s3client, *smallStart, *smallEnd, *smallCount, *blockSize)
				case MODE_BS:
					readRandomLargeFileBlock(ctx, worker, cycle, s3client, *blockSize)
				}
			}
		}(worker)
	}
	wg.Wait()
}

func runAIWorker(ctx context.Context, worker, cycle int, client *s3.Client, smallStart, smallEnd, smallCount, blockSizeMiB int) {
	var wg sync.WaitGroup
	// Read 1 random small file
	smallIdx := rand.Intn(smallEnd-smallStart+1) + smallStart
	smallFile := fmt.Sprintf("small/file_%08d.txt", smallIdx)
	readSmallFile(ctx, client, worker, cycle, smallFile)

	// Read smallCount random small files in parallel
	wg.Add(smallCount)
	for _ = range smallCount {
		idx := rand.Intn(smallEnd-smallStart+1) + smallStart
		smallFile = fmt.Sprintf("small/file_%08d.txt", idx)
		go func(smallFile string) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			default:
			}

			readSmallFile(ctx, client, worker, cycle, smallFile)

			select {
			case <-ctx.Done():
				return
			default:
			}

			readRandomLargeFileBlock(ctx, worker, cycle, client, blockSizeMiB)

		}(smallFile)
	}
	wg.Wait()

}

func readRandomLargeFileBlock(ctx context.Context, worker int, cycle int, client *s3.Client, blockSizeMiB int) {
	const largeMaxIndex = 50000
	fileIdx := rand.Intn(largeMaxIndex) + 1
	const largeSize = 100 * 1024 * 1024
	start := rand.Intn(largeSize - blockSizeMiB*1024*1024)
	end := start + 1024*1024*blockSizeMiB
	file := fmt.Sprintf("large/file_%08d.txt", fileIdx)
	readLargeRange(ctx, client, worker, cycle, file, start, end)
}

func readSmallFile(ctx context.Context, client *s3.Client, wid, cycle int, key string) {
	_ = atomic.AddInt64(&readSmallRunning, 1)
	start := time.Now()
	var out *s3.GetObjectOutput
	var err error
	for {
		out, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
		})

		elapsed := time.Since(start)
		var errQuotaExceed ratelimit.QuotaExceededError
		var errMaxAttempts *retry.MaxAttemptsError

		if errors.As(err, &errQuotaExceed) || errors.As(err, &errMaxAttempts) {
			errorLogger.Printf("Failed to read small %s: %v in %s. Retrying", key, err, elapsed)
			continue
		} else if err == nil {
			_ = atomic.AddUint64(&requestCountSmall, 1)
			break
		} else {
			requestCountSmall := atomic.AddUint64(&requestCountSmall, 1)
			simultaneous := atomic.AddInt64(&readSmallRunning, -1)
			failureCountSmall := atomic.AddUint64(&failureCountSmall, 1)
			errorLogger.Printf("Failed to read small %s: %v, simultaneous %v, failed %v of %v", key, err,
				simultaneous,
				failureCountSmall,
				requestCountSmall)
			return
		}
	}
	defer out.Body.Close()

	readBytes, err := io.Copy(io.Discard, out.Body)
	if err != nil {
		errorLogger.Printf("Failed to read body of small %s: %v", key, err)
	}
	elapsed := time.Since(start)
	elapsedScript := time.Since(scriptStartTime)
	totalBytesRead := atomic.AddUint64(&totalBytesRead, uint64(readBytes))
	speed := float64(readBytes) / elapsed.Seconds() / 1024 / 1024
	totalSpeed := float64(totalBytesRead) / elapsedScript.Seconds() / 1024 / 1024

	simultaneous := atomic.AddInt64(&readSmallRunning, -1)
	log.Printf(
		"[W%d] Cycle %d: small %s in %s (this %.2f MB/s, total %.2f MB/s) simultaneous %v, failed %v of %v",
		wid,
		cycle,
		key,
		elapsed,
		speed,
		totalSpeed,
		simultaneous,
		atomic.LoadUint64(&failureCountSmall),
		atomic.LoadUint64(&requestCountSmall),
	)
}

func readLargeRange(ctx context.Context, client *s3.Client, wid, cycle int, key string, startByte, endByte int) {
	_ = atomic.AddInt64(&readLargeRunning, 1)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	var out *s3.GetObjectOutput
	var err error
	start := time.Now()
	for {
		out, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
			Range:  aws.String(rangeHeader),
		})

		elapsed := time.Since(start)
		var errQuotaExceeded ratelimit.QuotaExceededError
		var errMaxAttempts *retry.MaxAttemptsError
		if errors.As(err, &errQuotaExceeded) || errors.As(err, &errMaxAttempts) {
			errorLogger.Printf("Failed to read large %s: %v in %s. Retrying", key, err, elapsed)
			continue
		} else if err == nil {
			_ = atomic.AddUint64(&requestCountLarge, 1)
			break
		} else {

			simultaneous := atomic.AddInt64(&readLargeRunning, -1)
			errorLogger.Printf("Failed to read large %s: %v", key, err)
			failureCountLarge := atomic.AddUint64(&failureCountLarge, 1)
			requestCountLarge := atomic.AddUint64(&requestCountLarge, 1)
			errorLogger.Printf(
				"[W%d] Cycle %d: range %s (0 bytes) in %s (N/A MB/s) simultaneous %v, failed %v of %v",
				wid,
				cycle,
				key,
				elapsed,
				simultaneous,
				failureCountLarge,
				requestCountLarge,
			)
			return
		}
	}

	elapsed := time.Since(start)
	elapsedScript := time.Since(scriptStartTime)

	defer out.Body.Close()
	readBytes, err := io.Copy(io.Discard, out.Body)
	if err != nil {
		errorLogger.Printf("Failed to read body of range %s: %v", key, err)
	}

	totalBytesRead := atomic.AddUint64(&totalBytesRead, uint64(readBytes))

	speed := float64(readBytes) / elapsed.Seconds() / 1024 / 1024
	totalSpeed := float64(totalBytesRead) / elapsedScript.Seconds() / 1024 / 1024

	simultaneous := atomic.AddInt64(&readLargeRunning, -1)
	log.Printf(
		"[W%d] Cycle %d: range %s (%d bytes) in %s (this %.2f MB/s, total %.2f MB/s) simultaneous %v, failed %v of %v",
		wid,
		cycle,
		key,
		readBytes,
		elapsed,
		speed,
		totalSpeed,
		simultaneous,
		atomic.LoadUint64(&failureCountLarge),
		atomic.LoadUint64(&requestCountLarge),
	)
}
