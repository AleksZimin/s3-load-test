package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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
	workers            = 100
	printProgressEvery = 512 // How often to update the percentage output
)

func deleteWorker(
	ctx context.Context,
	minioClient *minio.Client,
	log *zap.Logger,
	prefix string,
	jobs <-chan int,
	counter *int64,
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

			// Check if file exists
			if !force {
				_, err := minioClient.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
				// skip if file does not exist
				if err != nil {
					if minio.ToErrorResponse(err).Code == "NoSuchKey" {
						log.Debug("file does not exist, skipping", zap.Error(err))
						continue
					}
					log.Error("StatObject error", zap.Error(err))
					continue
				}
				log.Debug("file exists, deleting", zap.Error(err))
			}

			err := minioClient.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
			if err != nil {
				log.Error("RemoveObject error", zap.Error(err))
				continue
			} else {
				log.Debug("file deleted successfully")
				done := atomic.AddInt64(counter, 1)
				elapsed := time.Since(startTime).Seconds()
				percent := float64(done) / float64(total) * 100
				log.Debug(
					"deleted",
					zap.Int64("done", done),
					zap.Int64("total", total),
					zap.Float64("percent", percent),
					zap.Float64("elapsed", elapsed),
				)
				if done%printProgressEvery == 0 || done == int64(total) {
					rate := float64(done) / elapsed
					remaining := float64(total) - float64(done)
					eta := time.Duration(remaining/rate) * time.Second
					log.Sugar().Infof("[PROGRESS] %.2f%% (%d/%d), ETA: %s\n", percent, done, total, eta.Truncate(time.Second))
				}
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
		fmt.Println(`Usage: <program> <prefix/> <start> <end> [--force]
Example: 0 10000 1K
- <prefix>: Prefix for the files to delete. Can be "large/" or "small/".
- <start>: Starting index of files to delete
- <end>: Ending index of files to delete
- [--force]: Optional flag to not check if files exist before deleting. `)
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

	force := false
	if len(os.Args) < 5 {
		log.Info("No force flag provided, will check if files exist before deleting")
	} else if len(os.Args) > 5 {
		log.Fatal("Too many arguments. Expected 4 or 5 arguments, got", zap.Int("count", len(os.Args)))
	}
	if len(os.Args) == 5 && os.Args[4] == "" {
		log.Info("No force flag provided, will check if files exist before deleting")
	} else if len(os.Args) == 5 && os.Args[4] != "--force" {
		log.Fatal("Invalid key. Only --force supported for fourth argument", zap.String("have", os.Args[4]))
	}
	if len(os.Args) == 5 && os.Args[4] == "--force" {
		force = true
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
	startTime := time.Now()

	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deleteWorker(ctx, minioClient, log.With(zap.Int("worker", worker)), prefix, jobs, &counter, total, startTime, force)
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
	log.Info("delete completed")
}
