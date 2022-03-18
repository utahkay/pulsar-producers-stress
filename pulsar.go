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

func (p PulsarClient) startTest(tenant string, namespace string) {
	var numTopics = config.GetInt("numTopics")
	var numProducersPerTopic = config.GetInt("numProducersPerTopic")
	var messageIntervalMs = config.GetInt("messageIntervalMs")
	var startProducerIntervalMs = config.GetInt("startProducerIntervalMs")

	log.Printf("INFO Creating %d producers on %d topics", numProducersPerTopic*numTopics, numTopics)
	for tp := 0; tp < numProducersPerTopic; tp++ {
		for t := 0; t < numTopics; t++ {
			topic := fmt.Sprintf("%s/%s/topic-%d", tenant, namespace, t)
			producerIndex := tp*numTopics + t
			go p.runOneProducer(producerIndex, topic, messageIntervalMs)
			time.Sleep(time.Duration(startProducerIntervalMs) * time.Millisecond)
		}
	}
}

func (p PulsarClient) runOneProducer(producerIndex int, topic string, messageIntervalMs int) {
	// Create producer
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		log.Printf("WARN runOneProducer [%d] failed to create producer: %s", producerIndex, err)
		return
	}
	log.Printf("INFO runOneProducer Created producer %d successfully", producerIndex)
	numberProducersTotal.Inc()

	logEveryNthMessage := (30 * 1000) / messageIntervalMs

	// Produce messages forever
	for i := 0; ; i++ {
		msg := fmt.Sprintf("message-%d", i)

		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{Payload: []byte(msg)})
		if err != nil {
			log.Printf("WARN runOneProducer [%d] failed to produce message [%d]: %s", producerIndex, i, err)
		} else {
			messageProducedTotal.Inc()
			if i%logEveryNthMessage == 0 {
				log.Printf("INFO runOneProducer Producer [%d] has produced %d messages", producerIndex, i)
			}
		}

		time.Sleep(time.Duration(messageIntervalMs) * time.Millisecond)
	}
}
