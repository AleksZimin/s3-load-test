package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	S3_ENDPOINT = "http://10.200.0.72:18080"
	// accessKeyID        = "XRX3Q4ZK8ANGDV4L3W21"
	// secretAccessKey    = "WcDJBkQs7BLGLaQDPzjEXEIXoFuM5K0R1dnu5Kbk"
	useSSL             = false
	bucketName         = "test-bucket"
	workers            = 500
	printProgressEvery = 512
	S3_REGION          = "us-east-1"
	S3_BUCKET          = "test-bucket"
	S3_ACCESS_KEY      = "XRX3Q4ZK8ANGDV4L3W21"
	S3_SECRET_KEY      = "WcDJBkQs7BLGLaQDPzjEXEIXoFuM5K0R1dnu5Kbk"
)

func main() {
	s3Endpoint := flag.String("endpoint-url", S3_ENDPOINT, "S3 endpoint URL")
	timeoutSeconds := flag.Int("connection-timeout", 60, "Connection timeout in seconds")
	flag.Parse()

	if len(os.Args) < 5 {
		fmt.Println("Usage: <program> <prefix> <start> <end> <size> [-connection-timeout=10] [--force]")
		os.Exit(1)
	}

	log, err := createLogger(filepath.Join(".", "log.log"))
	if err != nil {
		fmt.Printf("Failed to create logger: %v", err)
		os.Exit(1)
	}

	prefix := os.Args[1]
	start, _ := strconv.Atoi(os.Args[2])
	end, _ := strconv.Atoi(os.Args[3])
	sizeBytes, _ := parseSize(os.Args[4])
	force := len(os.Args) == 6 && os.Args[5] == "--force"

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		cancel()
	}()

	// cfg, err := config.LoadDefaultConfig(ctx,
	// 	config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	// 	config.WithRegion("us-east-1"),
	// 	config.WithEndpointResolverWithOptions(
	// 		aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
	// 			return aws.Endpoint{
	// 				URL:               endpoint,
	// 				SigningRegion:     "us-east-1",
	// 				HostnameImmutable: true,
	// 			}, nil
	// 		}),
	// 	),
	// )

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(S3_REGION),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(S3_ACCESS_KEY, S3_SECRET_KEY, "")),
		config.WithHTTPClient(&http.Client{
			Timeout: time.Second * time.Duration(*timeoutSeconds),
		}),
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
	})

	found := false
	out, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.Fatal("Failed to list buckets", zap.Error(err))
	}
	for _, b := range out.Buckets {
		if aws.ToString(b.Name) == bucketName {
			found = true
			break
		}
	}
	if !found {
		_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			log.Fatal("Create bucket failed", zap.Error(err))
		}
	}

	total := int64(end - start)
	jobs := make(chan int, workers*10)
	var wg sync.WaitGroup
	var counter, skipped, uploaded, errors int64
	startTime := time.Now()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			uploadWorker(ctx, s3Client, log.With(zap.Int("worker", id)), prefix, jobs, sizeBytes, &counter, &uploaded, &skipped, &errors, total, startTime, force)
		}(i)
	}

	for i := start; i < end; i++ {
		select {
		case <-ctx.Done():
			break
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
	errorCounter *int64,
	total int64,
	startTime time.Time,
	force bool,
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
			objectName := fmt.Sprintf("%sfile_%08d.txt", prefix, job)
			log := log.With(zap.String("name", objectName), zap.Int("job", job))

			needUpload := true
			if !force {
				_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(bucketName),
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
					Bucket:        aws.String(bucketName),
					Key:           aws.String(objectName),
					Body:          bytes.NewReader(buf),
					ContentLength: aws.Int64(contentLength),
					ContentType:   aws.String("application/octet-stream"),
				})

				if err != nil {
					log.Error("upload error", zap.Error(err))
					atomic.AddInt64(errorCounter, 1)
				} else {
					log.Debug("file uploaded successfully")
					atomic.AddInt64(uploadedCounter, 1)
				}
			}

			processed := atomic.AddInt64(processedCounter, 1)
			skipped := atomic.LoadInt64(skippedCounter)
			errors := atomic.LoadInt64(errorCounter)
			uploaded := atomic.LoadInt64(uploadedCounter)
			elapsed := time.Since(startTime).Seconds()
			percent := float64(processed) / float64(total) * 100
			if processed%printProgressEvery == 0 || processed == total {
				rate := float64(processed) / elapsed
				remaining := float64(total) - float64(processed)
				eta := time.Duration(remaining/rate) * time.Second
				log.Sugar().Infof("[PROGRESS] %.2f%% (%d/%d), Uploaded: %d, Skipped: %d, Errors: %d, Rate: %.2f/s, ETA: %s",
					percent, processed, total, uploaded, skipped, errors, rate, eta.Truncate(time.Second))
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
