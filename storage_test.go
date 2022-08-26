package gcs

import (
	"errors"
	"os"
	"path"
	"time"

	"cloud.google.com/go/storage"
	"github.com/haoxins/g"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Storage", func() {
	It("Download should work", func() {
		c := Client{
			Bucket:                "gcp-public-data-nexrad-l2",
			Timeout:               time.Second * 10,
			WithoutAuthentication: true,
		}

		// gs://gcp-public-data-nexrad-l2/2012/07/23/KMUX/NWS_NEXRAD_NXL2DP_KMUX_20120723000000_20120723005959.tar
		written, err := c.Download("test.tar", "2012/07/23/KMUX/NWS_NEXRAD_NXL2DP_KMUX_20120723000000_20120723005959.tar")
		Expect(err).To(BeNil())
		Expect(written).To(BeNumerically(">", 0))

		os.Remove("test.tar")
	})

	It("Download should not create file if the object does not exist", func() {
		c := Client{
			Bucket:                "gcp-public-data-nexrad-l2",
			Timeout:               time.Second * 10,
			WithoutAuthentication: true,
		}

		written, err := c.Download("not-exists.tar", "not-exists.tar")
		Expect(err).To(BeNil())
		Expect(written).To(BeNumerically("==", 0))
	})

	It("ReadString and WriteString should work", func() {
		bucket := os.Getenv("GCS_BUCKET")
		if bucket == "" {
			Skip("Skip because GCS_BUCKET is not set")
		}

		c := Client{
			Bucket:                bucket,
			Timeout:               time.Second * 10,
			WithoutAuthentication: false,
		}

		file := "haoxins-gcs-package-test.txt"
		value := g.String(time.Now().Unix())

		s, e := c.ReadString("not-exists.txt")
		Expect(errors.Is(e, storage.ErrObjectNotExist)).To(BeTrue())
		Expect(s).To(BeEmpty())

		e = c.WriteString(file, value)
		Expect(e).To(BeNil())

		s, e = c.ReadString(file)
		Expect(e).To(BeNil())
		Expect(s).To(Equal(value))

		e = c.WriteString(file, "  \n\n \t\t"+value+"\t\t \n\n  ")
		Expect(e).To(BeNil())

		s = c.ReadStringTrim(file)
		Expect(s).To(Equal(value))

		e = c.Delete(file)
		Expect(e).To(BeNil())
	})

	It("Delete should work", func() {
		bucket := os.Getenv("GCS_BUCKET")
		if bucket == "" {
			Skip("Skip because GCS_BUCKET is not set")
		}

		c := Client{
			Bucket:                bucket,
			Timeout:               time.Second * 10,
			WithoutAuthentication: false,
		}

		dir := g.String(time.Now().Unix())
		file := path.Join(dir, "1.txt")
		value := "666"

		e := c.WriteString(file, value)
		Expect(e).To(BeNil())

		s, e := c.ReadString(file)
		Expect(e).To(BeNil())
		Expect(s).To(Equal(value))

		e = c.Delete(file)
		Expect(e).To(BeNil())

		s, e = c.ReadString(file)
		Expect(errors.Is(e, storage.ErrObjectNotExist)).To(BeTrue())
		Expect(s).To(BeEmpty())
	})
})
