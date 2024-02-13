package queue

import "github.com/prometheus/client_golang/prometheus"

type MetricsCollector struct {
	inFlightDescr *prometheus.Desc
	queuedDescr   *prometheus.Desc
	manager       *Manager
}

// NewMetricsCollector constructor.
func NewMetricsCollector(m *Manager) *MetricsCollector {
	if m == nil {
		panic("manager required")
	}
	return &MetricsCollector{
		manager:       m,
		inFlightDescr: prometheus.NewDesc("requests_queue_inflight", "Number of requests in flight", []string{"deployment"}, nil),
		queuedDescr:   prometheus.NewDesc("requests_queue", "Number of request queued", []string{"deployment"}, nil),
	}
}

// MustRegister registers all metrics.
func (p *MetricsCollector) MustRegister(r prometheus.Registerer) {
	r.MustRegister(p)
}

// Describe sends the super-set of all possible descriptors of metrics.
func (p *MetricsCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- p.inFlightDescr
	descs <- p.queuedDescr
}

// Collect is called by the Prometheus registry when collecting metrics.
func (p *MetricsCollector) Collect(c chan<- prometheus.Metric) {
	for deployment, v := range p.manager.TotalCounts() {
		c <- prometheus.MustNewConstMetric(p.queuedDescr, prometheus.GaugeValue, float64(v), deployment)
	}
	for deployment, v := range p.manager.InProgressCount() {
		c <- prometheus.MustNewConstMetric(p.inFlightDescr, prometheus.GaugeValue, float64(v), deployment)
	}
}
