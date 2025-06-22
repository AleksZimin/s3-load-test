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
	readSmallRunning       int64 = 0
	readLargeRunning       int64 = 0
	errorLogger            *log.Logger
	scenarioLogger         *log.Logger
	S3_ENDPOINT            string = "https://10.210.0.67:19443"
	S3_ENDPOINT_WITH_CACHE string = "https://10.210.0.67:19444"

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

	MODE_AI          = "ai"
	MODE_SIMPLE      = "simple"
	MODE_ALLSCENARIO = "all-scenarios"
)

func main() {
	MODES := fmt.Sprintf("(%s|%s|%s)", MODE_AI, MODE_SIMPLE, MODE_ALLSCENARIO)

	mode := flag.String("mode", MODE_AI, fmt.Sprintf("Select mode (%s)", MODES))
	s3Endpoint := flag.String("endpoint-url", S3_ENDPOINT, "S3 endpoint URL")
	s3EndpointWithCache := flag.String("cache-endpoint-url", S3_ENDPOINT_WITH_CACHE, "S3 endpoint URL with cache")
	workers := flag.Int("workers", 10, "Number of parallel workers")
	smallStart := flag.Int("small-start", 0, "Start of small file range")
	smallEnd := flag.Int("small-end", 0, "End of small file range")
	smallCount := flag.Int("small-count", 1000, "Number of small files to read simultaneously")
	cycles := flag.Int("cycles", -1, "Number of cycles per worker")
	rangeSizeMb := flag.Int("range-size-mb", 10, "Large file download range size")
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
			- Read range of <range-size-mb> size in random location of large/largefile_<rand(small-start/1000, small-end/1000)>.txt, assuming large file size 100Mb
		- simple:
		  - Read range of <range-size-mb> size in random location of large/largefile_<rand(small-start/1000, small-end/1000)>.txt, assuming large file size 100Mb
		
		Common options:
		- workers = amount of simultaneously running workers. Must be > 0.
		- cycles = number of cycles per worker (default 0 - infinite)
		- connection-timeout = Connection timeout in seconds. 0 or not set for disabling timeout`, MODES)
		os.Exit(1)
	}

	// logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY, 0644)
	logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("failed to open log file: %v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()

	scenarioFile, err := os.OpenFile("scenario.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("failed to open scenario log file: %v\n", err)
		os.Exit(1)
	}
	defer scenarioFile.Close()

	errorLogger = log.New(logFile, "ERROR: ", log.LstdFlags|log.Lmicroseconds)
	scenarioLogger = log.New(scenarioFile, "", log.LstdFlags|log.Lmicroseconds)

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
		config.WithHTTPClient(&http.Client{
			Timeout: time.Second * time.Duration(*timeoutSeconds),
		}),
	)

	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
		errorLogger.Printf("Failed to load AWS config: %v", err)
		os.Exit(1)
	}

	retryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.RateLimiter = ratelimit.None // Disable rate limiting
	})

	s3client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
		o.Retryer = retryer
		o.BaseEndpoint = aws.String(*s3Endpoint)
	})

	s3clientWithCache := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
		o.Retryer = retryer
		o.BaseEndpoint = aws.String(*s3EndpointWithCache)
	})

	scriptStartTime = time.Now()

	switch *mode {
	case MODE_ALLSCENARIO:

		timeToLoad := 30 * time.Minute
		timeSleep := 15 * time.Minute

		runScenario(ctx, "ai-workers-10-range1", s3client, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *smallCount, 1, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		runScenario(ctx, "ai-workers-20-range1", s3client, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *smallCount, 1, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		runScenario(ctx, "ai-workers-10-range10", s3client, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *smallCount, 10, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		runScenario(ctx, "ai-workers-20-range10", s3client, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *smallCount, 10, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		runScenario(ctx, "ai-workers-10-range1-with-cache", s3clientWithCache, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *smallCount, 1, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		runScenario(ctx, "ai-workers-20-range1-with-cache", s3clientWithCache, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *smallCount, 1, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		runScenario(ctx, "ai-workers-10-range10-with-cache", s3clientWithCache, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *smallCount, 10, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		runScenario(ctx, "ai-workers-20-range10-with-cache", s3clientWithCache, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *smallCount, 10, timeToLoad)
	default:
		runLoadTest(ctx, s3client, *mode, *workers, *cycles, *smallStart, *smallEnd, *smallCount, *rangeSizeMb)
	}
}

func runLoadTest(ctx context.Context, client *s3.Client, mode string, workers, cycles, smallStart, smallEnd, smallCount, rangeSizeMb int) {
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()

			for cycle := 1; cycles < 0 || cycle <= cycles; cycle++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				switch mode {
				case MODE_AI:
					runAIWorker(ctx, worker, cycle, client, smallStart, smallEnd, smallCount, rangeSizeMb)
				case MODE_SIMPLE:
					idx := rand.Intn(smallEnd-smallStart+1) + smallStart
					largeFile := fmt.Sprintf("large/file_%08d.txt", idx/1000)

					readRandomLargeFileRange(ctx, worker, cycle, client, rangeSizeMb, largeFile)
				}
			}
		}(worker)
	}
	wg.Wait()
}

func runAIWorker(ctx context.Context, worker, cycle int, client *s3.Client, smallStart, smallEnd, smallCount, rangeSizeMiB int) {
	var wg sync.WaitGroup
	// Read 1 random small file
	smallIdx := rand.Intn(smallEnd-smallStart+1) + smallStart
	smallFile := fmt.Sprintf("small/file_%08d.txt", smallIdx)
	readSmallFile(ctx, client, worker, cycle, smallFile)

	// Read smallCount random small files in parallel
	wg.Add(smallCount)
	for _ = range smallCount {
		idx := rand.Intn(smallEnd-smallStart+1) + smallStart

		go func(idx int) {
			defer wg.Done()

			smallFile = fmt.Sprintf("small/file_%08d.txt", idx)
			largeFile := fmt.Sprintf("large/file_%08d.txt", idx/1000)

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

			readRandomLargeFileRange(ctx, worker, cycle, client, rangeSizeMiB, largeFile)

		}(idx)
	}
	wg.Wait()

}

func readRandomLargeFileRange(ctx context.Context, worker int, cycle int, client *s3.Client, rangeSizeMiB int, largeFile string) {
	// const largeMaxIndex = 50000
	// fileIdx := rand.Intn(largeMaxIndex) + 1
	// file := fmt.Sprintf("large/file_%08d.txt", fileIdx)
	const largeSize = 100 * 1024 * 1024
	start := rand.Intn(largeSize - rangeSizeMiB*1024*1024)
	end := start + 1024*1024*rangeSizeMiB

	readLargeRange(ctx, client, worker, cycle, largeFile, start, end)
}

func readSmallFile(ctx context.Context, client *s3.Client, wid, cycle int, key string) {
	_ = atomic.AddInt64(&readSmallRunning, 1)
	start := time.Now()
	var out *s3.GetObjectOutput
	var err error
	attempt := 0
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
			timeSleep := 5 * time.Second
			if attempt < 10 {
				timeSleep = min(time.Duration(100*(1<<attempt))*time.Millisecond, 5*time.Second)
				attempt++
			}
			errorLogger.Printf("Sleeping for %s before retrying small %s", timeSleep, key)
			time.Sleep(timeSleep)
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
	if atomic.LoadUint64(&requestCountSmall)%1000 == 0 {
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
}

func readLargeRange(ctx context.Context, client *s3.Client, wid, cycle int, key string, startByte, endByte int) {
	_ = atomic.AddInt64(&readLargeRunning, 1)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	var out *s3.GetObjectOutput
	var err error
	start := time.Now()
	attempt := 0
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
			timeSleep := 5 * time.Second
			if attempt < 10 {
				timeSleep = min(time.Duration(100*(1<<attempt))*time.Millisecond, 5*time.Second)
				attempt++
			}
			errorLogger.Printf("Sleeping for %s before retrying large %s", timeSleep, key)
			time.Sleep(timeSleep)
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
	if atomic.LoadUint64(&requestCountLarge)%1000 == 0 {
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
}

func runScenario(parentCtx context.Context, name string, client *s3.Client, mode string, workers, cycles, smallStart, smallEnd, smallCount, rangeSizeMb int, duration time.Duration) {
	scenarioLogger.Printf("Start %s: mode=%s workers=%d range=%dMB, url=%s", name, mode, workers, rangeSizeMb, *client.Options().BaseEndpoint)
	startTime := time.Now()
	startSmall := atomic.LoadUint64(&requestCountSmall)
	startLarge := atomic.LoadUint64(&requestCountLarge)
	startBytes := atomic.LoadUint64(&totalBytesRead)

	scriptStartTime = startTime
	ctx, cancel := context.WithTimeout(parentCtx, duration)
	runLoadTest(ctx, client, mode, workers, cycles, smallStart, smallEnd, smallCount, rangeSizeMb)
	cancel()

	elapsed := time.Since(startTime)
	smallDone := atomic.LoadUint64(&requestCountSmall) - startSmall
	largeDone := atomic.LoadUint64(&requestCountLarge) - startLarge
	bytesDone := atomic.LoadUint64(&totalBytesRead) - startBytes
	speed := float64(bytesDone) / elapsed.Seconds() / 1024 / 1024

	scenarioLogger.Printf("Finish %s: duration=%s small=%d large=%d bytes=%d avgSpeed=%.2f MB/s", name, elapsed, smallDone, largeDone, bytesDone, speed)
}

func waitWithContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
