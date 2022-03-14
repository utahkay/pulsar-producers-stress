package main

import (
	"fmt"
	"strings"

	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/streamnative/pulsarctl/pkg/auth"
	ctl "github.com/streamnative/pulsarctl/pkg/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/common"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

type OauthConfig struct {
	IssuerUrl               string
	Audience                string
	AdminCredentialsFileUrl string
}

type Config struct {
	AdminServiceUrl string
	Oauth           *OauthConfig
}

func main() {
	config := &Config{
		AdminServiceUrl: "https://kay-1.test-kay-johansen.test.sn2.dev",
		Oauth: &OauthConfig{
			IssuerUrl:               "https://auth.test.cloud.gcp.streamnative.dev/",
			Audience:                "urn:sn:pulsar:test-kay-johansen:kay-1",
			AdminCredentialsFileUrl: "file:///Users/kayjohansen/service-account/test-kay-johansen-super-admin-test.json",
		},
	}
	client, err := getPulsarClient(*config)
	if err != nil {
		panic(err)
	}

	fmt.Println("Created client successfully")

	err = doTest(client)
	if err != nil {
		panic(err)
	}

	err = cleanup(client)
	if err != nil {
		panic(err)
	}

	fmt.Println("Test completed successfully")
}

func doTest(client ctl.Client) error {
	err := client.Tenants().Create(utils.TenantData{
		Name:            "private",
		AllowedClusters: []string{"kay-1"},
	})
	if err != nil && !strings.Contains(err.Error(), "409 reason: Tenant already exist") {
		return err
	}

	err = client.Namespaces().CreateNamespace("private/test")
	if err != nil && !strings.Contains(err.Error(), "409 reason: Namespace already exist") {
		return err
	}

	topicName, err := utils.GetTopicName("private/test/topic-1")
	if err != nil {
		return err
	}

	err = client.Topics().Create(*topicName, 0)
	if err != nil {
		if strings.Contains(err.Error(), "409 reason: This topic already exist") {
			fmt.Printf("The topic %s already exists\n", topicName)
		} else {
			return err
		}
	}

	return nil
}

func cleanup(client ctl.Client) error {
	nsName, err := utils.GetNameSpaceName("private", "test")
	if err != nil {
		return err
	}

	partitionedTopics, nonPartitionedTopics, err := client.Topics().List(*nsName)
	if err != nil {
		return err
	}
	fmt.Println("Partitioned topics:")
	fmt.Println(partitionedTopics)
	fmt.Println("Non-partitioned topics:")
	fmt.Println(nonPartitionedTopics)

	for _, t := range partitionedTopics {
		fmt.Printf("Deleting partitioned topic %s\n", t)
		err = deleteTopic(client, t, true)
		if err != nil {
			return err
		}
	}
	for _, t := range nonPartitionedTopics {
		fmt.Printf("Deleting non-partitioned topic %s\n", t)
		deleteTopic(client, t, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteTopic(client ctl.Client, topic string, partitioned bool) error {
	force := true
	nonPartitioned := !partitioned

	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return err
	}

	err = client.Topics().Delete(*topicName, force, nonPartitioned)
	if err != nil {
		return err
	}

	return nil
}

func getPulsarClient(config Config) (ctl.Client, error) {
	conf := &common.Config{
		WebServiceURL:    config.AdminServiceUrl,
		PulsarAPIVersion: common.V2,
	}
	issuer := oauth2.Issuer{
		IssuerEndpoint: config.Oauth.IssuerUrl,
		Audience:       config.Oauth.Audience,
	}
	keyFile := config.Oauth.AdminCredentialsFileUrl
	oauth2Auth, err := auth.NewAuthenticationOAuth2WithDefaultFlow(issuer, keyFile)
	if err != nil {
		return nil, err
	}
	return ctl.NewWithAuthProvider(conf, oauth2Auth), nil
}
