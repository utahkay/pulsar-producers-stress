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

type AdminClient struct {
	client ctl.Client
}

type AdminConfig struct {
	AdminServiceUrl string
	Oauth           *OauthConfig
}

func newAdmin(config AdminConfig) (AdminClient, error) {
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
		return AdminClient{}, err
	}

	client, err := ctl.NewPulsarClientWithAuthProvider(conf, oauth2Auth)
	if err != nil {
		return AdminClient{}, err
	}

	return AdminClient{client: client}, nil
}

func (admin AdminClient) createNamespace(tenant string, namespace string, cluster string, role string) error {
	err := admin.client.Tenants().Create(utils.TenantData{
		Name:            tenant,
		AllowedClusters: []string{cluster},
	})
	if err != nil && !strings.Contains(err.Error(), "409 reason: Tenant already exist") {
		return err
	}

	err = admin.client.Namespaces().CreateNamespace(fmt.Sprintf("%s/%s", tenant, namespace))
	if err != nil && !strings.Contains(err.Error(), "409 reason: Namespace already exist") {
		return err
	}

	if len(role) != 0 {
		return admin.grantNamespacePermissionsToRole(tenant, namespace, role)
	}

	return nil
}

func (admin AdminClient) grantNamespacePermissionsToRole(tenant string, namespace string, role string) error {
	nsName, err := utils.GetNameSpaceName(tenant, namespace)
	if err != nil {
		return err
	}

	return admin.client.Namespaces().GrantNamespacePermission(*nsName, role, []common.AuthAction{"produce", "consume"})
}

func (admin AdminClient) cleanupTopics(tenant string, namespace string) error {
	nsName, err := utils.GetNameSpaceName(tenant, namespace)
	if err != nil {
		return err
	}

	partitionedTopics, nonPartitionedTopics, err := admin.client.Topics().List(*nsName)
	if err != nil {
		return err
	}
	fmt.Println("Partitioned topics:")
	fmt.Println(partitionedTopics)
	fmt.Println("Non-partitioned topics:")
	fmt.Println(nonPartitionedTopics)

	for _, t := range partitionedTopics {
		fmt.Printf("Deleting partitioned topic %s\n", t)
		err = admin.deleteTopic(t, true)
		if err != nil {
			return err
		}
	}
	for _, t := range nonPartitionedTopics {
		fmt.Printf("Deleting non-partitioned topic %s\n", t)
		admin.deleteTopic(t, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func (admin AdminClient) deleteTopic(topic string, partitioned bool) error {
	force := true
	nonPartitioned := !partitioned

	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return err
	}

	err = admin.client.Topics().Delete(*topicName, force, nonPartitioned)
	if err != nil {
		return err
	}

	return nil
}
