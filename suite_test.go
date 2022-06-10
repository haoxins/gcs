package gcs

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestGCS(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "GCS Suite")
}
