package gcp

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/storage"
)

// StorageClient is a client for Google Cloud Storage
type StorageClient struct {
	ProjectID string
	Timeout   time.Duration
	Bucket    string
}

func (c *StorageClient) Write(object string, content io.Reader) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	wc := client.Bucket(c.Bucket).Object(object).NewWriter(ctx)
	_, err = io.Copy(wc, content)
	if err != nil {
		return err
	}

	wc.Close()

	return nil
}

func (c *StorageClient) WriteString(object string, content string) error {
	return nil
}

func (c *StorageClient) ReadString(object string, content string) (string, error) {
	return "", nil
}
