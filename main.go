package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

type OauthConfig struct {
	IssuerUrl          string
	Audience           string
	CredentialsFileUrl string
}

type ClientConfig struct {
	ServiceUrl string
	Oauth      *OauthConfig
}

var (
	config  *viper.Viper
	cleanup = flag.Bool("cleanup", false, "Delete all topics on the namespace")

	messageProducedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "message_produced_total",
		Help: "Counter of number of messages produced",
	})

	numberProducersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "number_producers_total",
		Help: "Counter of number of producers currently active",
	})
)

func main() {
	var cluster = config.GetString("target")
	var tenant = config.GetString("tenant")
	var namespace = config.GetString("namespace")
	var role = config.GetString("heartbeatServiceAccount")

	flag.Parse()
	if *cleanup {
		log.Printf("INFO Cleanup selected; will delete all topics in namespace %s/%s", tenant, namespace)
	}

	// Use the admin service account to set up the namespace
	admin, err := newAdmin(ClientConfig{
		ServiceUrl: config.GetString("pulsarClient.serviceUrlAdmin"),
		Oauth: &OauthConfig{
			IssuerUrl:          config.GetString("pulsarClient.oauthIssuerUrl"),
			Audience:           config.GetString("pulsarClient.oauthAudience"),
			CredentialsFileUrl: config.GetString("pulsarClient.oauthCredentialsUrlAdmin"),
		},
	})
	if err != nil {
		log.Fatalf("ERROR Failed to create admin client: %v\n", err)
	}
	log.Println("INFO Created admin client successfully")

	// Create tenant and namespace, and grant permissions to heartbeat service account
	if err = admin.createNamespace(tenant, namespace, cluster, role); err != nil {
		log.Fatalf("ERROR Failed to create namespace: %v\n", err)
	}
	log.Println("INFO Created namespace successfully")

	// Delete all existing topics on namespace to start clean
	if err := admin.cleanupTopics(tenant, namespace); err != nil {
		log.Printf("WARN Unable to delete topics to be able to start with an empty namespace: %s", err)
	}

	if *cleanup {
		log.Println("INFO Cleanup complete")
		os.Exit(0)
	}

	// Use the heartbeat service account to produce messages
	pulsarClient, err := newPulsarClient(ClientConfig{
		ServiceUrl: config.GetString("pulsarClient.serviceUrl"),
		Oauth: &OauthConfig{
			IssuerUrl:          config.GetString("pulsarClient.oauthIssuerUrl"),
			Audience:           config.GetString("pulsarClient.oauthAudience"),
			CredentialsFileUrl: config.GetString("pulsarClient.oauthCredentialsUrl"),
		},
	})
	if err != nil {
		log.Fatalf("ERROR Failed to create Pulsar client: %v\n", err)
	}
	log.Println("INFO Created Pulsar client successfully")

	// Run numProducersPerTopic on each of numTopics topics
	log.Println("INFO Starting the test")
	go pulsarClient.startTest(tenant, namespace)

	// Run forever, while serving prometheus metrics
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":8080", nil)
}

func init() {
	log.Println("INFO Loading config.")
	config = viper.New()
	config.SetConfigName("application")
	config.SetConfigType("yaml")
	config.AddConfigPath(".")
	config.AddConfigPath("/var/config/")
	if err := config.ReadInConfig(); err != nil {
		panic(err)
	}
}
