package aws

import (
	"errors"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Configuration contains parameters required to connect to kinesis
type Configuration struct {
	Region      string
	Endpoint    string
	Credentials *credentials.Credentials
}

// NewSession creates a new aws session
func NewSession(configuration Configuration) (*session.Session, error) {
	awssession, err := session.NewSession(&aws.Config{
		Region:      &configuration.Region,
		Endpoint:    &configuration.Endpoint,
		Credentials: configuration.Credentials,
	})
	if err != nil {
		log.Println(
			"msg", "could not create aws session",
			"region", configuration.Region,
			"endpoint", configuration.Endpoint,
			"error", err,
		)
		return nil, errors.New("could not create aws session")
	}

	return awssession, nil
}

// NewKinesisClient creates a new aws kinesis client.
func NewKinesisClient(awssession *session.Session) *kinesis.Kinesis {
	return kinesis.New(awssession)
}
