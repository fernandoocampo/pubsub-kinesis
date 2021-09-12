package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Client contains data to connect to dynamo service
type Client struct {
	dynamoDBClient *dynamodb.DynamoDB
}

// NewClient creates a new dynamodb client.
func NewClient(awssession *session.Session) *Client {
	newDynamoDB := dynamodb.New(awssession)
	newClient := Client{
		dynamoDBClient: newDynamoDB,
	}
	return &newClient
}
