package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	endpoint           = "localhost:19443"
	accessKeyID        = "minioadmin"
	secretAccessKey    = "minio-strong-secret"
	useSSL             = true
	bucketName         = "test-bucket"
	workers            = 500
	printProgressEvery = 512 // How often to update the percentage output
)

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
	minioClient *minio.Client,
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
			log := log.With(
				zap.String("name", objectName),
				zap.Int("job", job),
				zap.String("bucket", bucketName),
			)

			needUpload := true
			// Check if file exists
			if !force {
				_, err := minioClient.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
				if err == nil {
					needUpload = false
					_ = atomic.AddInt64(skippedCounter, 1)
					// done := atomic.LoadInt64(uploadcounter)
					// if skippedCounter%printProgressEvery == 0 {
					// 	log.Sugar().Infof("[SKIPPED] %s already exists, skipping upload. Total skipped: %d\n", objectName, skippedCounter)
					// }
					// continue
				}
			}

			if needUpload {
				_, err := minioClient.PutObject(
					ctx,
					bucketName,
					objectName,
					io.LimitReader(rand.Reader, contentLength),
					contentLength,
					minio.PutObjectOptions{},
				)
				if err != nil {
					log.Error("upload error", zap.Error(err))
					// Increment error counter
					_ = atomic.AddInt64(errorCounter, 1)
				} else {
					log.Debug("file uploaded successfully")
					// Increment uploaded counter
					_ = atomic.AddInt64(uploadedCounter, 1)
				}
			}

			// Update and print progress
			processed := atomic.AddInt64(processedCounter, 1)
			skippedCounter := atomic.LoadInt64(skippedCounter)
			errorCounter := atomic.LoadInt64(errorCounter)
			uploadedCounter := atomic.LoadInt64(uploadedCounter)
			elapsed := time.Since(startTime).Seconds()
			percent := float64(processed) / float64(total) * 100
			if processed%printProgressEvery == 0 || processed == int64(total) {
				rate := float64(processed) / elapsed
				remaining := float64(total) - float64(processed)
				eta := time.Duration(remaining/rate) * time.Second
				log.Sugar().Infof("[PROGRESS] %.2f%% (%d/%d), Uploaded: %d, Skipped: %d, Errors: %d, Rate: %.2f/s, ETA: %s\n",
					percent, processed, total, uploadedCounter, skippedCounter, errorCounter, rate,
					eta.Truncate(time.Second),
				)
			}
		}

	}
}

func createLogger(logFilePath string) (*zap.Logger, error) {
	// Open the log file
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// Create a zapcore.EncoderConfig
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Create an encoder
	encoder := zapcore.NewConsoleEncoder(encoderConfig)

	// Create the cores for file and stderr
	fileCore := zapcore.NewCore(encoder, zapcore.AddSync(logFile), zapcore.DebugLevel)
	consoleCore := zapcore.NewCore(encoder, zapcore.AddSync(os.Stderr), zapcore.InfoLevel)

	// Combine them with zapcore.NewTee
	combinedCore := zapcore.NewTee(fileCore, consoleCore)

	// Create the logger
	return zap.New(combinedCore, zap.AddCaller()), nil
}

func main() {
	if len(os.Args) < 5 {
		fmt.Println(`Usage: <program> <start> <end> <size> [--force]
Example: 0 10000 1K
- <prefix>: Prefix for the files to upload. Can be "large/" or "small/".
- <start>: Starting index of files to upload
- <end>: Ending index of files to upload
- <size>: Size of each file (e.g., 1K, 10M, 1G)
- [--force]: Optional flag to overwrite existing files`)
		os.Exit(1)
	}

	log, err := createLogger(filepath.Join(".", "log.log"))
	if err != nil {
		fmt.Printf("Failed to create encoder: %v", err)
		os.Exit(1)
	}

	prefix := os.Args[1]
	if prefix != "large/" && prefix != "small/" {
		log.Fatal("Invalid prefix. Only 'large/' or 'small/' supported", zap.String("prefix", prefix))
	}

	start, err := strconv.Atoi(os.Args[2])
	if err != nil || start < 0 {
		log.Fatal("Invalid start index", zap.Error(err))
	}

	end, err := strconv.Atoi(os.Args[3])
	if err != nil || end > 150000000 || end <= start {
		log.Fatal("Invalid end index", zap.Error(err), zap.Int("start", start), zap.Int("end", end))
	}

	sizeBytes, err := parseSize(os.Args[4])
	if err != nil || sizeBytes <= 0 {
		log.Fatal("Invalid size", zap.Error(err), zap.Int64("size", sizeBytes))
	}

	force := false
	if len(os.Args) < 6 {
		log.Info("No force flag provided, will check if files exist before uploading")
	} else if len(os.Args) > 6 {
		log.Fatal("Too many arguments. Expected 5 or 6 arguments, got", zap.Int("count", len(os.Args)))
	}
	if len(os.Args) == 6 && os.Args[5] == "" {
		log.Info("No force flag provided, will check if files exist before uploading")
	} else if len(os.Args) == 6 && os.Args[5] != "--force" {
		log.Fatal("Invalid key. Only --force supported for fifth argument", zap.String("have", os.Args[5]))
	}
	if len(os.Args) == 6 && os.Args[5] == "--force" {
		log.Info("Force flag provided, will overwrite existing files")
	}

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatal("MinIO connection error", zap.Error(err))
	}

	// Ensure bucket exists
	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	// Setup signal handling for Ctrl+C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signalChan:
			log.Debug("Received shutdown signal. Cancelling context...")
			cancelFunc()
		case <-ctx.Done():
			// Context cancelled by other means
		}
	}()

	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		log.Fatal("BucketExists check failed", zap.Error(err))
	}
	if !exists {
		if err := minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
			log.Fatal("MakeBucket failed", zap.Error(err))
		}
	}

	total := int64(end - start)
	jobs := make(chan int, workers*10)
	var wg sync.WaitGroup
	var counter int64 = 0
	var skippedCounter int64 = 0
	var errorCounter int64 = 0
	var uploadedCounter int64 = 0
	startTime := time.Now()

	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			uploadWorker(ctx, minioClient, log.With(zap.Int("worker", worker)), prefix, jobs, sizeBytes, &counter, &skippedCounter, &uploadedCounter, &errorCounter, total, startTime, force)
		}()
	}

	for job := start; job < end; job++ {
		select {
		case <-ctx.Done():
			log.Debug("canceling job queue")
		case jobs <- job:
			log.Debug("Job scheduled", zap.Int("job", job))
		}
	}
	close(jobs)

	wg.Wait()
	log.Info("Upload completed")
}
