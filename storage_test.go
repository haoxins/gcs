package gcs

import (
	"os"
	"time"

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

		name := "haoxins-gcs-package-test.txt"
		value := g.String(time.Now().Unix())

		s := c.ReadString("not-exists.txt")
		Expect(s).To(BeEmpty())
		e := c.WriteString(name, value)
		Expect(e).To(BeNil())
		s = c.ReadString(name)
		Expect(s).To(Equal(value))
		e = c.WriteString(name, "  \n\n \t\t"+value+"\t\t \n\n  ")
		Expect(e).To(BeNil())
		s = c.ReadStringTrim(name)
		Expect(s).To(Equal(value))
	})
})
