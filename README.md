# pubsub-kinesis
Publishing and Subscribing Template Using Kinesis

## Description

This project is a proof of concept for kinesis subscribers and publishers using the KCL vmware library. It can be used as a template in your projects.


## Throughput

If your Amazon Kinesis Data Streams application receives provisioned-throughput exceptions, you should increase the provisioned throughput for the DynamoDB table. The KCL creates the table with a provisioned throughput of 10 reads per second and 10 writes per second, but this might not be sufficient for your application. For example, if your Amazon Kinesis Data Streams application does frequent checkpointing or operates on a stream that is composed of many shards, you might need more throughput.

For information about provisioned throughput in DynamoDB, see Read/Write Capacity Mode and Working with Tables and Data in the Amazon DynamoDB Developer Guide.

## Playing locally

* start localstack

```sh
docker-compose up --build
```

* stop localstack

```sh
docker-compose down
```

* create stream in kinesis

```sh
aws kinesis --endpoint-url http://localhost:4566 create-stream --region us-west-2 --stream-name messages  --shard-count 1
```

* list kinesis streams

```sh
aws kinesis list-streams --endpoint-url http://localhost:4566 --region us-west-2
```

* create dynamodb table

```sh
aws dynamodb create-table \
--table-name message-test-checkpoint \
--profile dynamo \
--attribute-definitions AttributeName=ApplicationName,AttributeType=S AttributeName=ShardID,AttributeType=S \
--key-schema AttributeName=ApplicationName,KeyType=HASH AttributeName=ShardID,KeyType=RANGE \
--provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
--endpoint-url http://localhost:4566 --region us-west-2
```

* read dynamodb table

```sh
aws dynamodb scan \
--table-name message-test-checkpoint \
--endpoint-url http://localhost:4566 \
--profile dynamo \
--region us-west-2
```

* list dynamodb tables

```sh
aws dynamodb list-tables \
--endpoint-url http://localhost:4566 \
--profile dynamo \
--region us-west-2
```

* put record manually

```sh
aws kinesis put-record \
--endpoint-url http://localhost:4566 \
--region us-west-2 \
--stream-name messages \
--data sampledatarecord \
--partition-key samplepartitionkey
```


## References

* Concepts

    - https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html

* Reference code

    - https://github.com/vmware/vmware-go-kcl/blob/515fb0c7c47e073f475806ec145df7eca2afbb62/clientlibrary/config/config.go#L161
    - https://github.com/vmware/vmware-go-kcl/blob/515fb0c7c47e073f475806ec145df7eca2afbb62/clientlibrary/config/config.go#L161
    - https://github.com/vmware/vmware-go-kcl/blob/515fb0c7c47e073f475806ec145df7eca2afbb62/clientlibrary/config/kcl-config.go#L124
    -   
