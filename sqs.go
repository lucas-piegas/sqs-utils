package sqs

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pkg/errors"
)

type QueueClient interface {
	CreateQueue(ctx context.Context, params *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

type Client struct {
	client QueueClient
}

func NewSqsClient(endpoint, region string) (*Client, error) {
	cfg, err := newConfigWithEndpoint(endpoint, region)
	if err != nil {
		return nil, err
	}

	client := sqs.NewFromConfig(cfg)
	return &Client{client}, nil
}

func (c *Client) NewQueue(ctx context.Context, queueName string) (string, error) {
	input := &sqs.CreateQueueInput{
		QueueName: &queueName,
		Attributes: map[string]string{
			"MessageRetentionPeriod": "86400",
		},
	}
	output, err := c.client.CreateQueue(ctx, input)
	if err != nil {
		return "", errors.Wrap(err, "unable to create queue")
	}

	return *output.QueueUrl, nil
}

func (c *Client) SendMessage(ctx context.Context, queueUrl string, messageBody []byte, delay int32) error {
	messageInput := &sqs.SendMessageInput{
		DelaySeconds: delay,
		MessageBody:  aws.String(string(messageBody)),
		QueueUrl:     &queueUrl,
	}
	_, err := c.client.SendMessage(ctx, messageInput)
	if err != nil {
		return errors.Wrap(err, "unable to send message to queue")
	}
	return nil
}

func (c *Client) GetMessages(ctx context.Context, queueUrl string, messagesToReceive, visibilityTimeout int32) {
	gMInput := &sqs.ReceiveMessageInput{
		MessageAttributeNames: []string{
			string(types.QueueAttributeNameAll),
		},
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: messagesToReceive,
		VisibilityTimeout:   visibilityTimeout,
	}

	msgResult, err := c.client.ReceiveMessage(ctx, gMInput)
	if err != nil {
		fmt.Println("Got an error receiving messages:")
		fmt.Println(err)
		return
	}

	if msgResult.Messages != nil {
		fmt.Println("Message Body: " + *msgResult.Messages[0].Body)
	} else {
		fmt.Println("No messages found")
	}
}

func newConfigWithEndpoint(endpoint, region string) (aws.Config, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, reg string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           endpoint,
			SigningRegion: region,
		}, nil

	})

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithEndpointResolverWithOptions(customResolver))
	if err != nil {
		return aws.Config{}, errors.Wrap(err, "unable to lead configuration")
	}
	return cfg, nil
}
