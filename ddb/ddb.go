package ddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Client struct {
	db *dynamodb.Client
}

func NewLocalClient(port int) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		// Overwrite the default endpoint with out local one
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: fmt.Sprintf("http://localhost:%d", port)}, nil
			})),
		// Manually set the credentials, we don't have ~/.aws/config set up yet
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "mockAccessKeyID",
				SecretAccessKey: "mockSecretAccessKey",
			},
		}),
	)
	if err != nil {
		return nil, err
	}

	db := dynamodb.NewFromConfig(cfg)

	return &Client{db: db}, nil
}

func (c *Client) TableExists(tableName string) (bool, error) {
	tables, err := c.db.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		return false, err
	}

	for _, n := range tables.TableNames {
		fmt.Println(n)
		if n == tableName {
			return true, nil
		}
	}

	return false, nil
}

func (c *Client) CreateUserTable() error {
	// Set up columns
	attrDefs := []types.AttributeDefinition{
		{
			AttributeName: aws.String("username"),
			AttributeType: types.ScalarAttributeTypeS,
		},
	}

	// Set up partition (primary) key
	keySchema := []types.KeySchemaElement{
		{
			AttributeName: aws.String("username"),
			KeyType:       types.KeyTypeHash, // Partition Key
		},
	}

	// Compile the input (request)
	input := &dynamodb.CreateTableInput{
		TableName:            aws.String("users"),
		AttributeDefinitions: attrDefs,
		KeySchema:            keySchema,
		BillingMode:          "PAY_PER_REQUEST",
	}

	// Create the table
	_, err := c.db.CreateTable(context.TODO(), input)

	return err
}
