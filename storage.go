package gcs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// Client is a client for Google Cloud Storage
type Client struct {
	Bucket                string
	Timeout               time.Duration
	WithoutAuthentication bool
}

func (c *Client) Download(dest string, object string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	gcsClient, err := c.newClient(ctx)
	if err != nil {
		return 0, err
	}
	defer gcsClient.Close()

	handle := gcsClient.Bucket(c.Bucket).Object(object)

	// Check if the object exists
	_, err = handle.Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// TODO - Clean up file if error
	dst, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer dst.Close()

	src, err := handle.NewReader(ctx)
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

func (c *Client) Write(object string, src io.Reader) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	gcsClient, err := c.newClient(ctx)
	if err != nil {
		return err
	}
	defer gcsClient.Close()

	sink := gcsClient.Bucket(c.Bucket).Object(object).NewWriter(ctx)
	_, err = io.Copy(sink, src)
	if err != nil {
		return err
	}

	sink.Close()

	return nil
}

func (c *Client) Read(object string, sink io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	gcsClient, err := c.newClient(ctx)
	if err != nil {
		return err
	}
	defer gcsClient.Close()

	src, err := gcsClient.Bucket(c.Bucket).Object(object).NewReader(ctx)
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

func (c *Client) WriteString(object string, content string) error {
	return c.Write(object, strings.NewReader(content))
}

func (c *Client) ReadString(object string) (string, error) {
	buf := new(bytes.Buffer)

	err := c.Read(object, buf)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (c *Client) ReadStringTrim(object string) string {
	s, e := c.ReadString(object)
	if e != nil {
		return ""
	}
	return strings.TrimSpace(s)
}

func (c *Client) newClient(ctx context.Context) (*storage.Client, error) {
	if c.WithoutAuthentication {
		return storage.NewClient(ctx, option.WithoutAuthentication())
	} else {
		return storage.NewClient(ctx)
	}
}
