package gcp

import (
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Storage", func() {
	It("Download should work", func() {
		s := StorageClient{
			Bucket:                "gcp-public-data-nexrad-l2",
			Timeout:               time.Second * 10,
			WithoutAuthentication: true,
		}

		// gs://gcp-public-data-nexrad-l2/2012/07/23/KMUX/NWS_NEXRAD_NXL2DP_KMUX_20120723000000_20120723005959.tar
		written, err := s.Download("test.tar", "2012/07/23/KMUX/NWS_NEXRAD_NXL2DP_KMUX_20120723000000_20120723005959.tar")
		Expect(err).To(BeNil())
		Expect(written).To(BeNumerically(">", 0))

		os.Remove("test.tar")
	})
})
