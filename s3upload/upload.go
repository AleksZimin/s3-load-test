package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	smithymw "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const (
	S3_ENDPOINT = "http://10.200.0.71:9000"
	// bucketName         = "test-bucket"
	workers            = 100
	printProgressEvery = 16
	S3_REGION          = "us-east-1"
	S3_BUCKET          = "test-bucket"
	S3_ACCESS_KEY      = "4W3X8FJPV47JPJLH25QK"
	S3_SECRET_KEY      = "pEZ4ugfBA1gTPm14XTnSL0IZFmeyWizRDToa8BzJ"
)

// Main
func main() {
	workers := flag.Int("workers", 10, "Number of parallel workers")
	start := flag.Int("start", 0, "Start of small file range")
	end := flag.Int("end", 1000, "End of small file range")
	timeoutSeconds := flag.Int("connection-timeout", 10, "Connection timeout in seconds")
	prefix := flag.String("prefix", "", "S3 object prefix")
	size := flag.String("size", "1K", "Size of each file (e.g., 1K, 1M, 1G)")
	s3Endpoint := flag.String("endpoint-url", S3_ENDPOINT, "S3 endpoint URL")
	s3Region := flag.String("region", S3_REGION, "S3 region")
	s3Bucket := flag.String("bucket", S3_BUCKET, "S3 bucket name")
	s3AccessKey := flag.String("access-key", S3_ACCESS_KEY, "S3 access key ID")
	s3SecretKey := flag.String("secret-key", S3_SECRET_KEY, "S3 secret access key")
	force := flag.Bool("force", false, "Force execution even if files exist")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	sizeBytes, _ := parseSize(*size)

	// Begin validations
	if *prefix == "" {
		fmt.Fprintln(os.Stderr, "Error: --prefix is required")
		flag.Usage()
		os.Exit(1)
	}

	if *start < 0 {
		fmt.Fprintln(os.Stderr, "Error: --start must be >= 0")
		flag.Usage()
		os.Exit(1)
	}

	if *end <= *start {
		fmt.Fprintln(os.Stderr, "Error: --end must be greater than --start")
		flag.Usage()
		os.Exit(1)
	}

	if *s3Endpoint != "" {
		if !strings.HasPrefix(*s3Endpoint, "http:") && !strings.HasPrefix(*s3Endpoint, "https:") {
			fmt.Fprintln(os.Stderr, "Error: --endpoint-url must start with http:// or https://")
			flag.Usage()
			os.Exit(1)
		}
		if _, err := url.ParseRequestURI(*s3Endpoint); err != nil {
			fmt.Fprintf(os.Stderr, "Error: --endpoint-url is invalid: %v\n", err)
			flag.Usage()
			os.Exit(1)
		}
	}

	// End validations

	log, err := createLogger(filepath.Join(".", "log.log"))
	if err != nil {
		fmt.Printf("Failed to create logger: %v", err)
		os.Exit(1)
	}

	ctx := context.Background()
	// _, cancel := context.WithCancel(ctx) // Create a cancellable context, previously used for ctx - reworked just to disable that
	// defer cancel()
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	// go func() {
	// 	<-signalChan
	// 	cancel()
	// }()

	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
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

	retryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.RateLimiter = ratelimit.None // Disable rate limiting
	})

	if err != nil {
		log.Fatal("AWS config error", zap.Error(err))
	}

	// s3Client := s3.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
		o.Retryer = retryer
		o.BaseEndpoint = aws.String(*s3Endpoint)

		o.APIOptions = append(o.APIOptions, func(stack *smithymw.Stack) error {
			return stack.Finalize.Add(&stripHostMW{}, smithymw.Before)
		})
	})

	found := false
	out, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.Fatal("Failed to list buckets", zap.Error(err))
	}
	for _, b := range out.Buckets {
		log.Debug("Found bucket", zap.String("name", aws.ToString(b.Name)))
		if aws.ToString(b.Name) == *s3Bucket {
			found = true
			break
		}
	}
	if !found {
		_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(*s3Bucket),
		})
		if err != nil {
			log.Fatal("Create bucket failed", zap.Error(err))
		}
	}

	total := int64(*end - *start)
	jobs := make(chan int, *workers*10)
	var wg sync.WaitGroup
	var counter, skipped, uploaded, errors, retries int64
	startTime := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			uploadWorker(ctx, s3Client, log.With(zap.Int("worker", id)), *prefix, jobs, sizeBytes,
				&counter, &uploaded, &skipped, &errors, &retries, total, startTime, *force, *s3Bucket)

		}(i)
	}

loop:
	for i := *start; i < *end; i++ {
		select {
		case <-ctx.Done():
			break loop
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	log.Info("Upload completed")
}

func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	multiplier := int64(1)
	unit := sizeStr[len(sizeStr)-1]
	switch unit {
	case 'K', 'k':
		multiplier = 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	case 'T', 't':
		multiplier = 1024 * 1024 * 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	}
	val, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

func uploadWorker(
	ctx context.Context,
	s3Client *s3.Client,
	log *zap.Logger,
	prefix string,
	jobs <-chan int,
	contentLength int64,
	processedCounter *int64,
	uploadedCounter *int64,
	skippedCounter *int64,
	retryCounter *int64,
	errorCounter *int64,
	total int64,
	startTime time.Time,
	force bool,
	s3Bucket string,
) {

	for {
		select {
		case <-ctx.Done():
			log.Debug("canceled. exiting worker")
			return
		case job, ok := <-jobs:
			if !ok {
				log.Debug("jobs channel closed. exiting worker")
				return
			}

			objectName := fmt.Sprintf("%s/file_%08d.txt", strings.TrimSuffix(prefix, "/"), job)
			log := log.With(zap.String("name", objectName), zap.Int("job", job))

			needUpload := true
			if !force {
				_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(s3Bucket),
					Key:    aws.String(objectName),
				})
				if err == nil {
					atomic.AddInt64(skippedCounter, 1)
					needUpload = false
				}
			}

			if needUpload {
				// _, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
				// 	Bucket:        aws.String(bucketName),
				// 	Key:           aws.String(objectName),
				// 	Body:          io.LimitReader(rand.Reader, contentLength),
				// 	ContentLength: aws.Int64(contentLength),
				// 	ContentType:   aws.String("application/octet-stream"),
				// })

				buf := make([]byte, contentLength)
				_, err := rand.Read(buf)
				if err != nil {
					log.Error("failed to generate content", zap.Error(err))
					atomic.AddInt64(errorCounter, 1)
					continue
				}
				_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
					Bucket:        aws.String(s3Bucket),
					Key:           aws.String(objectName),
					Body:          bytes.NewReader(buf),
					ContentLength: aws.Int64(contentLength),
					ContentType:   aws.String("application/octet-stream"),
				})

				if err != nil {
					log.Error("upload error", zap.Error(err))
					// add retry logic here
					retryCount := 0
					for retryCount < 5 {
						atomic.AddInt64(retryCounter, 1)
						time.Sleep(time.Millisecond * 500 * time.Duration(retryCount+1))
						_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
							Bucket:        aws.String(s3Bucket),
							Key:           aws.String(objectName),
							Body:          bytes.NewReader(buf),
							ContentLength: aws.Int64(contentLength),
							ContentType:   aws.String("application/octet-stream"),
						})
						if err == nil {
							log.Debug("file uploaded successfully after retry")
							atomic.AddInt64(uploadedCounter, 1)
							break
						}
						retryCount++
					}
					if err != nil {
						log.Error("file upload failed after retries", zap.Error(err))
						// Increment error counter if all retries failed
						atomic.AddInt64(errorCounter, 1)
					}
				} else {
					log.Debug("file uploaded successfully")
					atomic.AddInt64(uploadedCounter, 1)
				}
			}

			processed := atomic.AddInt64(processedCounter, 1)
			skipped := atomic.LoadInt64(skippedCounter)
			errors := atomic.LoadInt64(errorCounter)
			retries := atomic.LoadInt64(retryCounter)
			uploaded := atomic.LoadInt64(uploadedCounter)
			elapsed := time.Since(startTime).Seconds()
			percent := float64(processed) / float64(total) * 100
			if processed%printProgressEvery == 0 || processed == total {
				rate := float64(processed) / elapsed
				remaining := float64(total) - float64(processed)
				eta := time.Duration(remaining/rate) * time.Second
				log.Sugar().Infof("[PROGRESS] %.2f%% (%d/%d), Uploaded: %d, Skipped: %d, Errors: %d, Retries: %d, Rate: %.2f/s, ETA: %s",
					percent, processed, total, uploaded, skipped, errors, retries, rate, eta.Truncate(time.Second))
			}
		}
	}
}

func createLogger(logFilePath string) (*zap.Logger, error) {
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:      "time",
		LevelKey:     "level",
		MessageKey:   "msg",
		EncodeLevel:  zapcore.LowercaseLevelEncoder,
		EncodeTime:   zapcore.ISO8601TimeEncoder,
		EncodeCaller: zapcore.ShortCallerEncoder,
		LineEnding:   zapcore.DefaultLineEnding,
	}
	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	fileCore := zapcore.NewCore(encoder, zapcore.AddSync(logFile), zapcore.DebugLevel)
	consoleCore := zapcore.NewCore(encoder, zapcore.AddSync(os.Stderr), zapcore.InfoLevel)
	combinedCore := zapcore.NewTee(fileCore, consoleCore)
	return zap.New(combinedCore, zap.AddCaller()), nil
}

type stripHostMW struct{}

func (*stripHostMW) ID() string { return "StripHostHeader" }

func (*stripHostMW) HandleFinalize(
	ctx context.Context,
	in smithymw.FinalizeInput,
	next smithymw.FinalizeHandler,
) (out smithymw.FinalizeOutput, meta smithymw.Metadata, err error) {

	if req, ok := in.Request.(*smithyhttp.Request); ok {
		req.Header.Del("Host")
		req.Request.Host = ""
	}
	return next.HandleFinalize(ctx, in)
}
