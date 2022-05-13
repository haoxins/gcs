package gcp

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

// StorageClient is a client for Google Cloud Storage
type StorageClient struct {
	ProjectID string
	Bucket    string
	Timeout   time.Duration
	client    *storage.Client
}

func (c *StorageClient) Download(dest string, object string) (int64, error) {
	dst, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer dst.Close()

	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	src, err := c.client.Bucket(c.Bucket).Object(object).NewReader(ctx)
	if err != nil {
		return 0, err
	}
	written, err := io.Copy(dst, src)
	if err != nil {
		return 0, err
	}

	src.Close()

	return written, nil
}

func (c *StorageClient) Write(object string, src io.Reader) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	sink := c.client.Bucket(c.Bucket).Object(object).NewWriter(ctx)
	_, err := io.Copy(sink, src)
	if err != nil {
		return err
	}

	sink.Close()

	return nil
}

func (c *StorageClient) Read(object string, sink io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	src, err := c.client.Bucket(c.Bucket).Object(object).NewReader(ctx)
	if err != nil {
		return err
	}

	_, err = io.Copy(sink, src)
	if err != nil {
		return err
	}

	src.Close()

	return nil
}

func (c *StorageClient) WriteString(object string, content string) error {
	return c.Write(object, strings.NewReader(content))
}

func (c *StorageClient) ReadString(object string) (string, error) {
	buf := new(bytes.Buffer)
	err := c.Read(object, buf)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
