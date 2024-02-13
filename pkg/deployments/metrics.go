package deployments

import "github.com/prometheus/client_golang/prometheus"

type MetricsCollector struct {
	currentScaleDescr *prometheus.Desc
	minScaleDescr     *prometheus.Desc
	maxScaleDescr     *prometheus.Desc
	manager           *Manager
}

// NewMetricsCollector constructor.
func NewMetricsCollector(m *Manager) *MetricsCollector {
	if m == nil {
		panic("manager required")
	}
	return &MetricsCollector{
		manager:           m,
		currentScaleDescr: prometheus.NewDesc("current_model_backends", "Number of model backends currently deployed", []string{"model"}, nil),
		minScaleDescr:     prometheus.NewDesc("min_model_backends", "Max number of model backends to deploy", []string{"model"}, nil),
		maxScaleDescr:     prometheus.NewDesc("max_model_backends", "Min number of model backends to deploy", []string{"model"}, nil),
	}
}

// MustRegister registers all metrics.
func (p *MetricsCollector) MustRegister(r prometheus.Registerer) {
	r.MustRegister(p)
}

// Describe sends the super-set of all possible descriptors of metrics.
func (p *MetricsCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- p.currentScaleDescr
	descs <- p.minScaleDescr
	descs <- p.maxScaleDescr
}

// Collect is called by the Prometheus registry when collecting metrics.
func (p *MetricsCollector) Collect(c chan<- prometheus.Metric) {
	for model, status := range p.manager.getScalesSnapshot() {
		c <- prometheus.MustNewConstMetric(p.currentScaleDescr, prometheus.GaugeValue, float64(status.Current), model)
		c <- prometheus.MustNewConstMetric(p.minScaleDescr, prometheus.GaugeValue, float64(status.Min), model)
		c <- prometheus.MustNewConstMetric(p.maxScaleDescr, prometheus.GaugeValue, float64(status.Max), model)
	}
}
