package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
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
	readFullFileRunning          int64 = 0
	readLargeRunning             int64 = 0
	errorLogger                  *log.Logger
	scenarioLogger               *log.Logger
	csvLogger                    *log.Logger
	S3_ENDPOINT                  string = "http://127.0.0.1:9000"
	s3Bucket                     string
	BALANCER_ENDPOINT            string = "https://10.210.0.67:19443"
	BALANCER_ENDPOINT_WITH_CACHE string = "https://10.210.0.67:19444"
	progressEvery                uint64
	downloadType                 string

	scriptStartTime time.Time
	totalBytesRead  uint64 = 0

	requestCountReadFullFile uint64 = 0
	requestCountLarge        uint64 = 0

	failureCountReadFullFile uint64 = 0
	failureCountLarge        uint64 = 0
)

const (
	S3_REGION     = "us-east-1"
	S3_BUCKET     = "test-bucket"
	S3_ACCESS_KEY = "minioadmin"
	S3_SECRET_KEY = "minio-strong-secret"

	MODE_AI             = "ai"
	MODE_DOWNLOAD_RANGE = "download-range"
	MODE_DOWNLOAD_FULL  = "download-full"

	MODE_ALLSCENARIO = "all-scenarios"
)

func main() {
	MODES := fmt.Sprintf("(%s|%s|%s)", MODE_AI, MODE_DOWNLOAD_RANGE, MODE_ALLSCENARIO)

	mode := flag.String("mode", MODE_AI, fmt.Sprintf("Select mode (%s)", MODES))
	s3EndpointURL := flag.String("s3-endpoint-url", S3_ENDPOINT, "S3 endpoint URL")
	s3Region := flag.String("region", S3_REGION, "S3 region")
	flag.StringVar(&s3Bucket, "bucket", S3_BUCKET, "S3 bucket name")
	s3AccessKey := flag.String("access-key", S3_ACCESS_KEY, "S3 access key ID")
	s3SecretKey := flag.String("secret-key", S3_SECRET_KEY, "S3 secret access key")
	balancerEndpoint := flag.String("balancer-endpoint-url", BALANCER_ENDPOINT, "Balancer endpoint URL")
	// balancerEndpointWithCache := flag.String("balancer-with-cache-endpoint-url", BALANCER_ENDPOINT_WITH_CACHE, "Balancer endpoint URL with cache")
	workers := flag.Int("workers", 10, "Number of parallel workers")
	smallStart := flag.Int("small-start", 0, "Start of small file range")
	smallEnd := flag.Int("small-end", 0, "End of small file range")
	threadsAmount := flag.Int("small-threads-count", 1000, "Number threads to read small file simultaneously in AI mode")
	cycles := flag.Int("cycles", -1, "Number of cycles per worker")
	rangeSizeMb := flag.Int("range-size-mb", 10, "Large file download range size")
	timeoutSeconds := flag.Int("connection-timeout", 0, "Connection timeout in seconds")
	userFileNameSuffix := flag.String("file-suffix", time.Now().Format("2006-01-02_15-04"), "Suffix for log files (default is timestamp)")
	flag.Uint64Var(&progressEvery, "progress-every", 1000, "Log progress every N requests (default 1000)")
	flag.StringVar(&downloadType, "download-type", "large", "Download type (large or small)")
	maxConnsPerHost := flag.Int("max-conns-per-host", 200, "Maximum parallel TCP connections per host (analogue of nginx keepalive)")

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
		- download-range:
		  - Read range of <range-size-mb> size in random location of large/largefile_<rand(small-start/1000, small-end/1000)>.txt, assuming large file size 100Mb
		
		Common options:
		- workers = amount of simultaneously running workers. Must be > 0.
		- cycles = number of cycles per worker (default 0 - infinite)
		- connection-timeout = Connection timeout in seconds. 0 or not set for disabling timeout`, MODES)
		os.Exit(1)
	}

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("failed to get hostname: %v\n", err)
		os.Exit(1)
	}
	fileNameSuffix := fmt.Sprintf("%s-%s", *userFileNameSuffix, hostname)

	logFile, err := os.OpenFile(fmt.Sprintf("log-%s.log", fileNameSuffix), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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

	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:   false,
		MaxConnsPerHost:     *maxConnsPerHost,
		MaxIdleConns:        *maxConnsPerHost * 2,
		MaxIdleConnsPerHost: *maxConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}

	client := &http.Client{
		Timeout:   time.Second * time.Duration(*timeoutSeconds),
		Transport: transport,
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(*s3Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*s3AccessKey, *s3SecretKey, "")),
		config.WithHTTPClient(client),
	)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
		errorLogger.Printf("Failed to load AWS config: %v", err)
		os.Exit(1)
	}

	retryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.RateLimiter = ratelimit.None // Disable rate limiting
	})

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
		o.Retryer = retryer
		o.BaseEndpoint = aws.String(*s3EndpointURL)
	})

	s3clientWithBanacer := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
		o.Retryer = retryer
		o.BaseEndpoint = aws.String(*balancerEndpoint)
	})

	// s3clientWithBalancerAndCache := s3.NewFromConfig(cfg, func(o *s3.Options) {
	// 	o.UsePathStyle = true
	// 	o.DisableLogOutputChecksumValidationSkipped = true
	// 	o.Retryer = retryer
	// 	o.BaseEndpoint = aws.String(*balancerEndpointWithCache)
	// })

	scriptStartTime = time.Now()

	switch *mode {
	case MODE_ALLSCENARIO:
		scenarioFile, err := os.OpenFile(fmt.Sprintf("scenario-%s.log", fileNameSuffix), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("failed to open scenario log file: %v\n", err)
			os.Exit(1)
		}
		defer scenarioFile.Close()

		csvFile, err := os.OpenFile(fmt.Sprintf("csv-%s.csv", fileNameSuffix), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("failed to open CSV log file: %v\n", err)
			os.Exit(1)
		}
		defer csvFile.Close()

		scenarioLogger = log.New(scenarioFile, "", log.LstdFlags|log.Lmicroseconds)
		// logger to generate CSV output without timestamps
		csvLogger = log.New(csvFile, "", 0)
		// Create header for CSV file
		csvLogger.Println("Scenario,Load type,Workers count,Range size for 100MB files (MB),Threads per worker,Start range for 1KB files,End range for 1KB files,Number of small files in range,Test duration (minutes),URL,Processed small files,Errors while processing small files,Processed large files,Errors while processing large files,Downloaded data (MB),Average download speed (MB/s)")

		timeToLoad := 10 * time.Minute
		timeSleep := 5 * time.Minute

		runScenario(ctx, "ai-workers-1-range1-with-balancer", s3clientWithBanacer, MODE_AI, 1, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		// timeToLoad := 10 * time.Second
		// timeSleep := 5 * time.Second

		// runScenario(ctx, "ai-workers-10-range1", s3Client, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		runScenario(ctx, "ai-workers-10-range1-with-balancer", s3clientWithBanacer, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		// runScenario(ctx, "ai-workers-10-range1-with-balancer-and-cache", s3clientWithBalancerAndCache, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "ai-workers-20-range1", s3Client, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		runScenario(ctx, "ai-workers-20-range1-with-balancer", s3clientWithBanacer, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		if ctx.Err() == nil {
			waitWithContext(ctx, timeSleep)
		}

		runScenario(ctx, "ai-workers-30-range1-with-balancer", s3clientWithBanacer, MODE_AI, 30, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)

		// runScenario(ctx, "ai-workers-20-range1-with-balancer-and-cache", s3clientWithBalancerAndCache, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "ai-workers-10-range10", s3Client, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "ai-workers-10-range10-with-balancer", s3clientWithBanacer, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "ai-workers-10-range10-with-balancer-and-cache", s3clientWithBalancerAndCache, MODE_AI, 10, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "ai-workers-20-range10", s3Client, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "ai-workers-20-range10-with-balancer", s3clientWithBanacer, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "ai-workers-20-range10-with-balancer-and-cache", s3clientWithBalancerAndCache, MODE_AI, 20, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-50-range1", s3Client, MODE_DOWNLOAD_RANGE, 50, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-1000-range1-with-balancer", s3clientWithBanacer, MODE_DOWNLOAD_RANGE, 1000, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-100-range1", s3Client, MODE_DOWNLOAD_RANGE, 100, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-100-range1-with-balancer", s3clientWithBanacer, MODE_DOWNLOAD_RANGE, 100, *cycles, *smallStart, *smallEnd, *threadsAmount, 1, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-50-range10", s3Client, MODE_DOWNLOAD_RANGE, 50, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-1000-range10-with-balancer", s3clientWithBanacer, MODE_DOWNLOAD_RANGE, 1000, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-100-range10", s3Client, MODE_DOWNLOAD_RANGE, 100, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-100-range10-with-balancer", s3clientWithBanacer, MODE_DOWNLOAD_RANGE, 100, *cycles, *smallStart, *smallEnd, *threadsAmount, 10, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-50-range99", s3Client, MODE_DOWNLOAD_RANGE, 50, *cycles, *smallStart, *smallEnd, *threadsAmount, 99, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-50-range99-with-balancer", s3clientWithBanacer, MODE_DOWNLOAD_RANGE, 50, *cycles, *smallStart, *smallEnd, *threadsAmount, 99, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-100-range99", s3Client, MODE_DOWNLOAD_RANGE, 100, *cycles, *smallStart, *smallEnd, *threadsAmount, 99, timeToLoad)
		// if ctx.Err() == nil {
		// 	waitWithContext(ctx, timeSleep)
		// }

		// runScenario(ctx, "download-range-workers-100-range99-with-balancer", s3clientWithBanacer, MODE_DOWNLOAD_RANGE, 100, *cycles, *smallStart, *smallEnd, *threadsAmount, 99, timeToLoad)
		scenarioLogger.Printf("All scenarios finished")
		return
	default:
		runLoadTest(ctx, s3Client, *mode, *workers, *cycles, *smallStart, *smallEnd, *threadsAmount, *rangeSizeMb)
	}
}

func runLoadTest(ctx context.Context, client *s3.Client, mode string, workers, cycles, smallStart, smallEnd, threadsAmount, rangeSizeMb int) {
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
					runAIWorker(ctx, worker, cycle, client, smallStart, smallEnd, threadsAmount, rangeSizeMb)
				case MODE_DOWNLOAD_RANGE:
					idx := rand.Intn(smallEnd-smallStart+1) + smallStart
					largeFile := fmt.Sprintf("large/file_%08d.txt", idx/1000)

					readRandomLargeFileRange(ctx, worker, cycle, client, rangeSizeMb, largeFile)
				case MODE_DOWNLOAD_FULL:
					idx := rand.Intn(smallEnd-smallStart+1) + smallStart
					var fileToDownload string
					switch downloadType {
					case "small":
						fileToDownload = fmt.Sprintf("small/file_%08d.txt", idx)
					case "large":
						fileToDownload = fmt.Sprintf("large/file_%08d.txt", idx/1000)
					}

					readFullFile(ctx, client, worker, cycle, fileToDownload)

				}
			}
		}(worker)
	}
	wg.Wait()
}

func runAIWorker(ctx context.Context, worker, cycle int, client *s3.Client, smallStart, smallEnd, threadsAmount, rangeSizeMiB int) {
	var wg sync.WaitGroup
	// Read 1 random small file
	smallIdx := rand.Intn(smallEnd-smallStart+1) + smallStart
	smallFile := fmt.Sprintf("small/file_%08d.txt", smallIdx)
	readFullFile(ctx, client, worker, cycle, smallFile)

	// Read threadsAmount random small files in parallel
	wg.Add(threadsAmount)
	for _ = range threadsAmount {
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

			readFullFile(ctx, client, worker, cycle, smallFile)

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
	const largeSize = 100 * 1024 * 1024
	start := rand.Intn(largeSize - rangeSizeMiB*1024*1024)
	end := start + 1024*1024*rangeSizeMiB

	readLargeRange(ctx, client, worker, cycle, largeFile, start, end)
}

func readFullFile(ctx context.Context, client *s3.Client, wid, cycle int, key string) {
	atomic.AddInt64(&readFullFileRunning, 1)
	start := time.Now()
	var out *s3.GetObjectOutput
	var err error
	attempt := 0
	for {
		out, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s3Bucket),
			Key:    aws.String(key),
		})

		elapsed := time.Since(start)
		var errQuotaExceed ratelimit.QuotaExceededError
		var errMaxAttempts *retry.MaxAttemptsError

		if errors.As(err, &errQuotaExceed) || errors.As(err, &errMaxAttempts) {
			errorLogger.Printf("Failed to read full file %s: %v in %s. Retrying", key, err, elapsed)
			timeSleep := 5 * time.Second
			if attempt < 10 {
				timeSleep = min(time.Duration(100*(1<<attempt))*time.Millisecond, 5*time.Second)
				attempt++
			}
			errorLogger.Printf("Sleeping for %s before retrying read full file %s", timeSleep, key)
			time.Sleep(timeSleep)
			continue
		} else if err == nil {
			atomic.AddUint64(&requestCountReadFullFile, 1)
			break
		} else {
			atomic.AddUint64(&requestCountReadFullFile, 1)
			simultaneous := atomic.AddInt64(&readFullFileRunning, -1)
			failureCountSmall := atomic.AddUint64(&failureCountReadFullFile, 1)
			errorLogger.Printf("Failed to read full file %s: %v, simultaneous %v, failed %v of %v", key, err,
				simultaneous,
				failureCountSmall,
				requestCountReadFullFile)
			return
		}
	}
	defer out.Body.Close()

	readBytes, err := io.Copy(io.Discard, out.Body)
	if err != nil {
		errorLogger.Printf("Failed to read body of full file %s: %v", key, err)
	}
	elapsed := time.Since(start)
	elapsedScript := time.Since(scriptStartTime)
	totalBytesRead := atomic.AddUint64(&totalBytesRead, uint64(readBytes))
	speed := float64(readBytes) / elapsed.Seconds() / 1024 / 1024
	totalSpeed := float64(totalBytesRead) / elapsedScript.Seconds() / 1024 / 1024

	simultaneous := atomic.AddInt64(&readFullFileRunning, -1)
	if atomic.LoadUint64(&requestCountReadFullFile)%progressEvery == 0 {
		log.Printf(
			"[W%d] Cycle %d: file %s (read %d B of total %.2f MiB) in %s of total %s (this %.2f MB/s, total %.2f MB/s) simultaneous %v, failed %v of %v",
			wid,
			cycle,
			key,
			readBytes,
			float64(totalBytesRead/1024/1024),
			elapsed,
			elapsedScript,
			speed,
			totalSpeed,
			simultaneous,
			atomic.LoadUint64(&failureCountReadFullFile),
			atomic.LoadUint64(&requestCountReadFullFile),
		)
	}
}

func readLargeRange(ctx context.Context, client *s3.Client, wid, cycle int, key string, startByte, endByte int) {
	atomic.AddInt64(&readLargeRunning, 1)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	var out *s3.GetObjectOutput
	var err error
	start := time.Now()
	attempt := 0
	for {
		out, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s3Bucket),
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
			atomic.AddUint64(&requestCountLarge, 1)
			break
		} else {

			simultaneous := atomic.AddInt64(&readLargeRunning, -1)
			errorLogger.Printf("Failed to read large %s: %v", key, err)
			failureCountLarge := atomic.AddUint64(&failureCountLarge, 1)
			requestCountLarge := atomic.AddUint64(&requestCountLarge, 1)
			errorLogger.Printf(
				"[W%d] Cycle %d: file %s (0 bytes) in %s (N/A MB/s) simultaneous %v, failed %v of %v",
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
	if atomic.LoadUint64(&requestCountLarge)%progressEvery == 0 {
		log.Printf(
			"[W%d] Cycle %d: file %s range: %s (read %.2f MiB of total %.2f MiB) in %s of total %s (this %.2f MB/s, total %.2f MB/s) simultaneous %v, failed %v of %v",
			wid,
			cycle,
			key,
			rangeHeader,
			float64(readBytes/1024/1024),
			float64(totalBytesRead/1024/1024),
			elapsed,
			elapsedScript,
			speed,
			totalSpeed,
			simultaneous,
			atomic.LoadUint64(&failureCountLarge),
			atomic.LoadUint64(&requestCountLarge),
		)
	}
}

func runScenario(parentCtx context.Context, name string, client *s3.Client, mode string, workers, cycles, rangeSmallStart, rangeSmallEnd, threadsAmount, rangeSizeMb int, duration time.Duration) {
	scenarioLogger.Printf("Start %s: mode=%s workers=%d range=%dMB, smallStart=%d smallEnd=%d threadsAmount=%d url=%s", name, mode, workers, rangeSizeMb, rangeSmallStart, rangeSmallEnd, threadsAmount, *client.Options().BaseEndpoint)
	startTime := time.Now()
	scriptStartTime = startTime

	// Reset counters
	atomic.StoreInt64(&readFullFileRunning, 0)
	atomic.StoreInt64(&readLargeRunning, 0)
	atomic.StoreUint64(&requestCountReadFullFile, 0)
	atomic.StoreUint64(&requestCountLarge, 0)
	atomic.StoreUint64(&totalBytesRead, 0)
	atomic.StoreUint64(&failureCountReadFullFile, 0)
	atomic.StoreUint64(&failureCountLarge, 0)

	ctx, cancel := context.WithTimeout(parentCtx, duration)
	runLoadTest(ctx, client, mode, workers, cycles, rangeSmallStart, rangeSmallEnd, threadsAmount, rangeSizeMb)
	cancel()

	elapsed := time.Since(startTime)
	// speed := float64(bytesDone) / elapsed.Seconds() / 1024 / 1024
	speed := float64(atomic.LoadUint64(&totalBytesRead)) / elapsed.Seconds() / 1024 / 1024

	scenarioLogger.Printf("Finish %s: duration=%s; small count total=%d; small count failed=%d; large count total=%d; large count failed=%d; megabytes=%d; avgSpeed=%.2f MB/s", name, elapsed, atomic.LoadUint64(&requestCountReadFullFile), atomic.LoadUint64(&failureCountReadFullFile), atomic.LoadUint64(&requestCountLarge), atomic.LoadUint64(&failureCountLarge), atomic.LoadUint64(&totalBytesRead)/1024/1024, speed)
	csvLogger.Printf("%s,%s,%d,%d,%d,%d,%d,%d,%.2f,%s,%d,%d,%d,%d,%.2f,%.2f",
		name,
		mode,
		workers,
		rangeSizeMb,
		threadsAmount,
		rangeSmallStart,
		rangeSmallEnd,
		rangeSmallEnd-rangeSmallStart+1,
		elapsed.Minutes(),
		*client.Options().BaseEndpoint,
		atomic.LoadUint64(&requestCountReadFullFile),
		atomic.LoadUint64(&failureCountReadFullFile),
		atomic.LoadUint64(&requestCountLarge),
		atomic.LoadUint64(&failureCountLarge),
		float64(atomic.LoadUint64(&totalBytesRead))/1024/1024,
		speed,
	)
}

func waitWithContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
