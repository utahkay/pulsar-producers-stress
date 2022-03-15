package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type OauthConfig struct {
	IssuerUrl               string
	Audience                string
	AdminCredentialsFileUrl string
}

type PulsarClientConfig struct {
	ServiceUrl string
	Oauth      *OauthConfig
}

var cluster = "kay-1"
var tenant = "private"
var namespace = "test"
var role = "test-kay-johansen@test-kay-johansen.auth.test.cloud.gcp.streamnative.dev"

func main() {
	admin, err := newAdmin(AdminConfig{
		AdminServiceUrl: "https://kay-1.test-kay-johansen.test.sn2.dev",
		Oauth: &OauthConfig{
			IssuerUrl:               "https://auth.test.cloud.gcp.streamnative.dev/",
			Audience:                "urn:sn:pulsar:test-kay-johansen:kay-1",
			AdminCredentialsFileUrl: "file:///Users/kayjohansen/service-account/test-kay-johansen-super-admin-test.json",
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Created admin client successfully")

	pulsarClient, err := newPulsarClient(PulsarClientConfig{
		ServiceUrl: "pulsar+ssl://kay-1.test-kay-johansen.test.sn2.dev:6651",
		Oauth: &OauthConfig{
			IssuerUrl:               "https://auth.test.cloud.gcp.streamnative.dev/",
			Audience:                "urn:sn:pulsar:test-kay-johansen:kay-1",
			AdminCredentialsFileUrl: "file:///Users/kayjohansen/service-account/test-kay-johansen-test.json",
		},
	})
	if err != nil {
		panic(err)
	}
	defer pulsarClient.Close()
	fmt.Println("Created Pulsar client successfully")

	if err = admin.createNamespace(tenant, namespace, cluster, role); err != nil {
		panic(err)
	}
	fmt.Println("Created namespace successfully")

	defer admin.cleanupTopics(tenant, namespace)

	for i := 1; i < 10; i++ {
		go produce(pulsarClient, i)
	}

	c := make(chan struct{})
	<-c
}

func produce(pulsarClient pulsar.Client, index int) error {
	topic := fmt.Sprintf("topic-%d", index)
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: fmt.Sprintf("%s/%s/%s", tenant, namespace, topic)})
	if err != nil {
		return err
	}
	fmt.Println("Created Pulsar producer successfully")

	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf("message-%d", i)
		msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(msg),
		})
		if err != nil {
			return err
		}
		log.Printf("[%d] Produced message msgId: %v -- content: '%s'", index, msgId, msg)
		time.Sleep(1 * time.Second)
	}

	return nil
}

func newPulsarClient(config PulsarClientConfig) (pulsar.Client, error) {
	oauth := pulsar.NewAuthenticationOAuth2(map[string]string{
		"type":       "client_credentials",
		"issuerUrl":  config.Oauth.IssuerUrl,
		"audience":   config.Oauth.Audience,
		"privateKey": config.Oauth.AdminCredentialsFileUrl,
	})
	return pulsar.NewClient(pulsar.ClientOptions{
		URL:            config.ServiceUrl,
		Authentication: oauth,
	})
}
