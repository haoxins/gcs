package gcp

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg4go/tools"
)

// StorageClient ...
type StorageClient struct {
	ProjectID string
	Timeout   time.Duration
}

func (c *StorageClient) Write(bucket, object string, content io.Reader) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	wc := client.Bucket(bucket).Object(object).NewWriter(ctx)
	_, err = io.Copy(wc, content)
	tools.AssertError(err)
	wc.Close()
}
