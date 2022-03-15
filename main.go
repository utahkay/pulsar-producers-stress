package main

import (
	"fmt"
)

type OauthConfig struct {
	IssuerUrl               string
	Audience                string
	AdminCredentialsFileUrl string
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

	fmt.Println("Created client successfully")

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
