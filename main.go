package main

import (
	"fmt"
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

var cluster = "kay-1"
var tenant = "private"
var namespace = "test"
var role = "test-kay-johansen@test-kay-johansen.auth.test.cloud.gcp.streamnative.dev"

func main() {
	admin, err := newAdmin(ClientConfig{
		ServiceUrl: "https://kay-1.test-kay-johansen.test.sn2.dev",
		Oauth: &OauthConfig{
			IssuerUrl:          "https://auth.test.cloud.gcp.streamnative.dev/",
			Audience:           "urn:sn:pulsar:test-kay-johansen:kay-1",
			CredentialsFileUrl: "file:///Users/kayjohansen/service-account/test-kay-johansen-super-admin-test.json",
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Created admin client successfully")

	pulsarClient, err := newPulsarClient(ClientConfig{
		ServiceUrl: "pulsar+ssl://kay-1.test-kay-johansen.test.sn2.dev:6651",
		Oauth: &OauthConfig{
			IssuerUrl:          "https://auth.test.cloud.gcp.streamnative.dev/",
			Audience:           "urn:sn:pulsar:test-kay-johansen:kay-1",
			CredentialsFileUrl: "file:///Users/kayjohansen/service-account/test-kay-johansen-test.json",
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Created Pulsar client successfully")

	if err = admin.createNamespace(tenant, namespace, cluster, role); err != nil {
		panic(err)
	}
	fmt.Println("Created namespace successfully")

	defer admin.cleanupTopics(tenant, namespace)

	for i := 1; i <= 10; i++ {
		go pulsarClient.produce(i)
	}

	c := make(chan struct{})
	<-c
}
