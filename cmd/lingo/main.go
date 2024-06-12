package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/sethvargo/go-envconfig"
	"github.com/substratusai/lingo/pkg/autoscaler"
	"github.com/substratusai/lingo/pkg/deployments"
	"github.com/substratusai/lingo/pkg/endpoints"
	"github.com/substratusai/lingo/pkg/leader"
	"github.com/substratusai/lingo/pkg/messenger"
	"github.com/substratusai/lingo/pkg/proxy"
	"github.com/substratusai/lingo/pkg/queue"
	"github.com/substratusai/lingo/pkg/stats"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}

func run() error {
	// Flags are only used to control logging.
	// TODO: Migrate to env variables.
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	var cfg struct {
		Hostname string `env:"HOSTNAME"`

		Namespace string `env:"NAMESPACE, default=default"`

		// Concurrency per replica.
		Concurrency int `env:"CONCURRENCY, default=100"`

		ScaleDownDelay int `env:"SCALE_DOWN_DELAY, default=30"`
		BackendRetries int `env:"BACKEND_RETRIES, default=1"`

		// MessengerURLs is a list of (comma-separated) URLs to listen for requests and send responses on.
		//
		// Format: <request-subscription1>|<response-topic1>,<request-subscription2>|<response-topic2>,...
		// You can optionally also specify the max number of handlers for each messenger URL pair.
		// If not specified, the default is 1000.
		// Format with setting max concurrency: <request-subscription1>|<response-topic1>|<max-handlers1>,
		//
		// Examples:
		//
		// Google PubSub:		"gcppubsub://projects/my-project/subscriptions/my-subscription|gcppubsub://projects/myproject/topics/mytopic"
		// with maxHandlers:	"gcppubsub://projects/my-project/subscriptions/my-subscription|gcppubsub://projects/myproject/topics/mytopic|1000"
		// Amazon SQS-to-SQS:	"awssqs://sqs.us-east-2.amazonaws.com/123456789012/myqueue1?region=us-east-2|awssqs://sqs.us-east-2.amazonaws.com/123456789012/myqueue2?region=us-east-2"
		// Amazon SQS-to-SNS:	"awssqs://sqs.us-east-2.amazonaws.com/123456789012/myqueue1?region=us-east-2|awssns:///arn:aws:sns:us-east-2:123456789012:mytopic?region=us-east-2"
		//  (NOTE: 3 slashes for SNS)
		// Azure Service Bus:	"azuresb://mytopic1?subscription=mysubscription|azuresb://mytopic2"
		// Rabbit MQ:			"rabbit://myqueue|rabbit://myexchange"
		// NATS:				"nats://example.mysubject1|nats://example.mysubject2"
		// Kafka:				"kafka://my-group?topic=my-topic1|kafka://my-topic2"
		MessengerURLs []string `env:"MESSENGER_URLS"`

		MetricsBindAddress     string `env:"METRICS_BIND_ADDRESS, default=:8082"`
		HealthProbeBindAddress string `env:"HEALTH_PROBE_BIND_ADDRESS, default=:8081"`
	}
	if err := envconfig.Process(context.Background(), &cfg); err != nil {
		return fmt.Errorf("parsing environment variables: %w", err)
	}

	type messengerURLPair struct {
		requests    string
		responses   string
		maxHandlers int
	}
	var messengerURLPairs []messengerURLPair
	for _, s := range cfg.MessengerURLs {
		parts := strings.Split(s, "|")
		if len(parts) != 2 && len(parts) != 3 {
			return fmt.Errorf("invalid subscription URL: %q", s)
		}
		var maxHandlers int = 1000
		var err error
		if len(parts) == 3 {
			maxHandlers, err = strconv.Atoi(parts[2])
			if err != nil {
				return fmt.Errorf("error converting messenger URL maxHandlers string %v to int: %w",
					parts[2], err)
			}
		}
		messengerURLPairs = append(messengerURLPairs, messengerURLPair{
			requests:    parts[0],
			responses:   parts[1],
			maxHandlers: maxHandlers,
		})
	}

	// TODO: Add Deployments to cache list.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				cfg.Namespace: {},
			},
		},
		Metrics: metricsserver.Options{
			BindAddress: cfg.MetricsBindAddress,
		},
		HealthProbeBindAddress: cfg.HealthProbeBindAddress,
		// LeaderElection is done in the Autoscaler.
		LeaderElection: false,
	})
	if err != nil {
		return fmt.Errorf("starting manager: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("clientset: %w", err)
	}
	ctx := ctrl.SetupSignalHandler()

	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("getting hostname: %v", err)
	}
	le := leader.NewElection(clientset, hostname, cfg.Namespace)

	queueManager := queue.NewManager(cfg.Concurrency)
	metricsRegistry := prometheus.WrapRegistererWithPrefix("lingo_", metrics.Registry)
	queue.NewMetricsCollector(queueManager).MustRegister(metricsRegistry)

	endpointManager, err := endpoints.NewManager(mgr)
	if err != nil {
		return fmt.Errorf("setting up endpoint manager: %w", err)
	}
	endpointManager.EndpointSizeCallback = queueManager.UpdateQueueSizeForReplicas
	// The autoscaling leader will scrape other lingo instances.
	// Exclude this instance from being scraped by itself.
	endpointManager.ExcludePods[hostname] = struct{}{}

	deploymentManager, err := deployments.NewManager(mgr)
	if err != nil {
		return fmt.Errorf("setting up deloyment manager: %w", err)
	}
	deploymentManager.Namespace = cfg.Namespace
	deploymentManager.ScaleDownPeriod = time.Duration(cfg.ScaleDownDelay) * time.Second
	deployments.NewMetricsCollector(deploymentManager).MustRegister(metricsRegistry)
	if err := mgr.AddReadyzCheck("readyz", deploymentManager.ReadinessChecker); err != nil {
		return fmt.Errorf("setup readiness handler: %w", err)
	}

	autoscaler, err := autoscaler.New(mgr)
	if err != nil {
		return fmt.Errorf("setting up autoscaler: %w", err)
	}
	autoscaler.Interval = 3 * time.Second
	autoscaler.AverageCount = 10 // 10 * 3 seconds = 30 sec avg
	autoscaler.LeaderElection = le
	autoscaler.Deployments = deploymentManager
	autoscaler.ConcurrencyPerReplica = cfg.Concurrency
	autoscaler.Queues = queueManager
	autoscaler.Endpoints = endpointManager
	go autoscaler.Start()

	proxy.MustRegister(metricsRegistry)
	proxyHandler := proxy.NewHandler(deploymentManager, endpointManager, queueManager)
	proxyHandler.MaxRetries = cfg.BackendRetries
	proxyServer := &http.Server{Addr: ":8080", Handler: proxyHandler}

	statsHandler := &stats.Handler{
		Queues: queueManager,
	}
	statsServer := &http.Server{Addr: ":8083", Handler: statsHandler}

	httpClient := &http.Client{
		Timeout: 30 * time.Minute,
	}
	var msgrs []*messenger.Messenger
	for i, msgURL := range messengerURLPairs {
		msgr, err := messenger.NewMessenger(
			ctx,
			msgURL.requests,
			msgURL.responses,
			msgURL.maxHandlers,
			deploymentManager,
			endpointManager,
			queueManager,
			httpClient,
			map[string]string{
				"lingo_hostname": cfg.Hostname,
			},
		)
		if err != nil {
			return fmt.Errorf("creating messenger[%d]: %w", i, err)
		}
		msgrs = append(msgrs, msgr)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			statsServer.Shutdown(context.Background())
			proxyServer.Shutdown(context.Background())
			wg.Done()
		}()
		if err := mgr.Start(ctx); err != nil {
			setupLog.Error(err, "problem running manager")
			os.Exit(1)
		}
	}()
	go func() {
		if err := statsServer.ListenAndServe(); err != nil {
			setupLog.Error(err, "error serving stats")
			os.Exit(1)
		}
	}()
	go func() {
		setupLog.Info("Starting leader election")
		err := le.Start(ctx)
		if err != nil {
			setupLog.Error(err, "starting leader election")
			os.Exit(1)
		}
	}()
	for i := range msgrs {
		go func() {
			setupLog.Info("Starting messenger", "index", i)
			err := msgrs[i].Start(ctx)
			if err != nil {
				setupLog.Error(err, "starting messenger")
				os.Exit(1)
			}
		}()
	}
	defer func() {
		setupLog.Info("waiting on manager to stop")
		wg.Wait()
		setupLog.Info("manager stopped")
	}()

	if ok := mgr.GetCache().WaitForCacheSync(ctx); !ok {
		return fmt.Errorf("client cache could not be synced")
	}
	if err := deploymentManager.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap deloyment manager: %w", err)
	}

	if err := proxyServer.ListenAndServe(); err != nil {
		return fmt.Errorf("listen and serve: %w", err)
	}

	for i := range msgrs {
		// TODO: Investigate if in-progress message handling will exit cleanly.
		// One concern is that responses might not be sent and messages may hang
		// in an un-Ack'd state if the context is cancelled.
		msgrs[i].Stop(ctx)
	}

	return nil
}
