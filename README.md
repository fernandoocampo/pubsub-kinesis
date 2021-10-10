# pubsub-kinesis
Publishing and Subscribing Template Using Kinesis

## Description

This project is a proof of concept for kinesis subscribers and publishers using the KCL vmware library. It can be used as a template in your projects.


## Throughput

If your Amazon Kinesis Data Streams application receives provisioned-throughput exceptions, you should increase the provisioned throughput for the DynamoDB table. The KCL creates the table with a provisioned throughput of 10 reads per second and 10 writes per second, but this might not be sufficient for your application. For example, if your Amazon Kinesis Data Streams application does frequent checkpointing or operates on a stream that is composed of many shards, you might need more throughput.

For information about provisioned throughput in DynamoDB, see Read/Write Capacity Mode and Working with Tables and Data in the Amazon DynamoDB Developer Guide.

## References

* Concepts

    - https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html

* Reference code

    - https://github.com/vmware/vmware-go-kcl/blob/515fb0c7c47e073f475806ec145df7eca2afbb62/clientlibrary/config/config.go#L161
    - https://github.com/vmware/vmware-go-kcl/blob/515fb0c7c47e073f475806ec145df7eca2afbb62/clientlibrary/config/config.go#L161
    - https://github.com/vmware/vmware-go-kcl/blob/515fb0c7c47e073f475806ec145df7eca2afbb62/clientlibrary/config/kcl-config.go#L124
    - https://github.com/vmware/vmware-go-kcl/blob/master/test/worker_test.go
