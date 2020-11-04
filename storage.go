package gcp

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/haoxins/tools/v2"
)

// StorageClient ...
type StorageClient struct {
	ProjectID string
	Timeout   time.Duration
	Bucket    string
}

func (c *StorageClient) Write(object string, content io.Reader) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	tools.PanicError(err)

	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	wc := client.Bucket(c.Bucket).Object(object).NewWriter(ctx)
	_, err = io.Copy(wc, content)
	tools.PanicError(err)

	wc.Close()
}
