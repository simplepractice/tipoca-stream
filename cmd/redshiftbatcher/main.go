package main

import (
	"context"
	"time"

	"flag"
	"github.com/spf13/cobra"
	pflag "github.com/spf13/pflag"
	"math/rand"
	"net/http"

	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/practo/klog/v2"
	conf "github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftbatcher/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftbatcher"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var rootCmd = &cobra.Command{
	Use:   "redshiftbatcher",
	Short: "Batches the debezium data, uploads to s3 and signals the load of the batch.",
	Long:  "Consumes the Kafka Topics, trasnform them for redshfit, batches them and uploads to s3. Also signals the load of the batch on successful batch and upload operation..",
	Run:   run,
}

func init() {
	klog.InitFlags(nil)
	rootCmd.PersistentFlags().String("config", "./cmd/redshiftbatcher/config/config.yaml", "config file")
	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("v"))
}

func serveMetrics() {
	klog.Info("Starting prometheus metric endpoint")
	http.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":8787", nil)
}

func run(cmd *cobra.Command, args []string) {
	klog.Info("Starting the redshift batcher")
	go serveMetrics()

	config, err := conf.LoadConfig(cmd)
	if err != nil {
		klog.Errorf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	var consumerGroups map[string]consumer.ConsumerGroupInterface
	var consumersReady []chan bool
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	for _, groupConfig := range config.ConsumerGroups {
		ready := make(chan bool)
		consumerGroup, err := consumer.NewConsumerGroup(
			groupConfig,
			redshiftbatcher.NewConsumer(ready),
		)
		if err != nil {
			klog.Errorf("Error making kafka consumer group, exiting: %v\n", err)
			os.Exit(1)
		}
		consumersReady = append(consumersReady, ready)
		groupID := groupConfig.GroupID
		consumerGroups[groupID] = consumerGroup
		klog.Infof("Succesfully created kafka client for group: %s", groupID)

		manager := consumer.NewManager(
			consumerGroup,
			groupID,
			groupConfig.TopicRegexes,
		)
		wg.Add(1)
		go manager.SyncTopics(ctx, 15, wg)
		wg.Add(1)
		go manager.Consume(ctx, wg)
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	ready := 0

	for ready >= 0 {
		select {
		case <-sigterm:
			klog.Info("Sigterm signal received")
			ready = -1
		}

		if ready == -1 || ready == len(consumersReady) {
			continue
		}

		for _, channel := range consumersReady {
			select {
			case <-channel:
				ready += 1
				klog.Infof("ConsumerGroup: %d is up and running", ready)
			}
		}
		klog.Info("Waiting for ConsumerGroups to come up...")
	}

	klog.Info("Cancelling context to trigger graceful shutdown...")
	cancel()

	// TODO: the processing function should signal back
	// It does not at present
	// https://github.com/practo/tipoca-stream/issues/18
	klog.Info("Waiting the some routines to gracefully shutdown (some don't)")
	time.Sleep(10 * time.Second)

	// routines which works with wait groups will shutdown gracefully
	wg.Wait()

	var closeErr error
	for groupID, consumerGroup := range consumerGroups {
		klog.Infof("Closing consumerGroup: %s", groupID)
		closeErr = consumerGroup.Close()
		if closeErr != nil {
			klog.Errorf(
				"Error closing consumer group: %s, err: %v", groupID, err)
		}
	}
	if closeErr != nil {
		os.Exit(1)
	}

	klog.Info("Goodbye!")
}

// main/main.main()
// => consumer/manager.Consume() => consumer/consumer_group.Consume()
// => sarama/consumer_group.Consume() => redshfitbatcher/consumer.ConsumeClaim()
func main() {
	rand.Seed(time.Now().UnixNano())

	rootCmd.Execute()
}
