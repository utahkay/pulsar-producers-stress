package main

import (
	"fmt"

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

	err = admin.createTopic("private", "test", "kay-1")
	if err != nil {
		panic(err)
	}

	err = admin.cleanupTopics("private", "test")
	if err != nil {
		panic(err)
	}

	fmt.Println("Test completed successfully")
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
