package hbbft_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var testingInstance *testing.T

func TestHbbft(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Hbbft Suite")
}
