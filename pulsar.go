package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type PulsarClient struct {
	client pulsar.Client
}

func newPulsarClient(config ClientConfig) (PulsarClient, error) {
	oauth := pulsar.NewAuthenticationOAuth2(map[string]string{
		"type":       "client_credentials",
		"issuerUrl":  config.Oauth.IssuerUrl,
		"audience":   config.Oauth.Audience,
		"privateKey": config.Oauth.CredentialsFileUrl,
	})
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            config.ServiceUrl,
		Authentication: oauth,
	})
	if err != nil {
		return PulsarClient{}, err
	}

	return PulsarClient{client: pulsarClient}, nil
}

func (p PulsarClient) produce(index int) error {
	topic := fmt.Sprintf("%s/%s/topic-%d", tenant, namespace, index)
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
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
