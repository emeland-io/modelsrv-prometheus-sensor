package config_test

import (
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"emeland.io/modelsrv-prometheus-sensor/internal/config"
)

func writeConfig(content string) string {
	dir := GinkgoT().TempDir()
	path := filepath.Join(dir, "sensor.yaml")
	Expect(os.WriteFile(path, []byte(content), 0o600)).To(Succeed())
	return path
}

var _ = Describe("config.Load", func() {
	It("loads a full valid config", func() {
		path := writeConfig(`
upstream: "http://modelsrv:24000/api/"
prometheusUrl: "http://prometheus:9090"
pollInterval: 15s
subscribers:
  - "http://downstream:24000/api/"
`)
		cfg, err := config.Load(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Upstream).To(Equal("http://modelsrv:24000/api/"))
		Expect(cfg.PrometheusURL).To(Equal("http://prometheus:9090"))
		Expect(cfg.PollInterval).To(Equal(15 * time.Second))
		Expect(cfg.Subscribers).To(Equal([]string{"http://downstream:24000/api/"}))
	})

	It("applies the default poll interval when unset", func() {
		path := writeConfig(`
upstream: "http://up:24000/api/"
prometheusUrl: "http://prom:9090"
`)
		cfg, err := config.Load(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.PollInterval).To(Equal(config.DefaultPollInterval))
	})

	It("trims a trailing slash from prometheusUrl and drops blank subscribers", func() {
		path := writeConfig(`
upstream: "http://up:24000/api/"
prometheusUrl: "http://prom:9090/"
subscribers:
  - "  "
  - "http://a:24000/api/"
`)
		cfg, err := config.Load(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.PrometheusURL).To(Equal("http://prom:9090"))
		Expect(cfg.Subscribers).To(Equal([]string{"http://a:24000/api/"}))
	})

	It("fails when upstream is missing", func() {
		path := writeConfig(`
prometheusUrl: "http://prom:9090"
`)
		_, err := config.Load(path)
		Expect(err).To(MatchError(ContainSubstring("missing upstream")))
	})

	It("fails when prometheusUrl is missing", func() {
		path := writeConfig(`
upstream: "http://up:24000/api/"
`)
		_, err := config.Load(path)
		Expect(err).To(MatchError(ContainSubstring("missing prometheusUrl")))
	})

	It("rejects a non-http upstream URL", func() {
		path := writeConfig(`
upstream: "ftp://up/api"
prometheusUrl: "http://prom:9090"
`)
		_, err := config.Load(path)
		Expect(err).To(MatchError(ContainSubstring("must use http or https")))
	})

	It("rejects an invalid subscriber URL", func() {
		path := writeConfig(`
upstream: "http://up:24000/api/"
prometheusUrl: "http://prom:9090"
subscribers:
  - "http://good/api/"
  - "://bad"
`)
		_, err := config.Load(path)
		Expect(err).To(MatchError(ContainSubstring("subscribers[1]")))
	})

	It("returns an error when the file does not exist", func() {
		_, err := config.Load(filepath.Join(GinkgoT().TempDir(), "nope.yaml"))
		Expect(err).To(HaveOccurred())
	})
})
