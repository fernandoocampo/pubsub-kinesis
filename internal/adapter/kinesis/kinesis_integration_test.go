package kinesis_test

import (
	"flag"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

var (
	integration      = flag.Bool("integration", false, "run integration tests")
	kinesisEndpoint  = flag.String("kinesisendpoint", "http://localhost:4566", "aws endpoint")
	dynamoDBEndpoint = flag.String("dynamodbendpoint", "http://localhost:4566", "aws endpoint")
	awsRegionName    = flag.String("awsregion", "us-west-2", "aws region")
)

func createAWSSession(region, endpoint string) (*session.Session, error) {
	log.Println("creating aws session", "region", *awsRegionName, "endpoint", endpoint)
	s, err := session.NewSession(&aws.Config{
		Region:   aws.String(region),
		Endpoint: aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebug),
		LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
		// Credentials: credentials.NewStaticCredentials("id", "sd", "idsd"),
		// Credentials: credentials.AnonymousCredentials,
		// DisableSSL:                aws.Bool(true),
		// S3ForcePathStyle:          aws.Bool(true),
		// DisableEndpointHostPrefix: aws.Bool(true),
		// Credentials: credentials.NewStaticCredentials("", "", ""),
	})
	return s, err
}
