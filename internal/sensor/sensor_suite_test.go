package sensor_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSensor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sensor Suite")
}
