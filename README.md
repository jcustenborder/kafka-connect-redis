# Introduction
[Documentation](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis) | [Download from the Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-redis)

The Redis plugin is a collection of connectors that are used to interact with a Redis cluster.

# Installation

## Confluent Hub

The following command can be used to install the plugin directly from the Confluent Hub using the
[Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html).

```bash
confluent-hub install jcustenborder/kafka-connect-redis:latest
```

## Manually

The zip file that is deployed to the [Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-redis) is available under
`target/components/packages/`. You can manually extract this zip file which includes all dependencies. All the dependencies
that are required to deploy the plugin are under `target/kafka-connect-target` as well. Make sure that you include all the dependencies that are required
to run the plugin.

1. Create a directory under the `plugin.path` on your Connect worker.
2. Copy all of the dependencies under the newly created subdirectory.
3. Restart the Connect worker.



# Source Connectors
## [Redis Pub/Sub Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sources/RedisPubSubSourceConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisPubSubSourceConnector
```

The Redis Pub/Sub Source Connector is used to retrieve data using Redis Pub/Sub subscriptions.


# Sink Connectors
## [Redis Pub/Sub Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sinks/RedisPubSubSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisPubSubSinkConnector
```

The Redis Pub/Sub Sink Connector is used to publish data from Kafka to Redis using Redis Pub/Sub subscriptions.
### Important

This connector expects records from Kafka to have a key and value that are stored as bytes or a string. If your data is already in Kafka in the format that you want in Redis consider using the ByteArrayConverter or the StringConverter for this connector. Keep in this does not need to be configured in the worker properties and can be configured at the connector level. If your data is not sitting in Kafka in the format you wish to persist in Redis consider using a Single Message Transformation to convert the data to a byte or string representation before it is written to Redis.
### Note

This connector ignores deletes. If the record stored in Kafka has a null value, this connector will drop the record. Only the data in the value of the Kafka record will be written to Redis.

## [Redis Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sinks/RedisSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisSinkConnector
```

The Redis Sink Connector is used to write data from Kafka to a Redis cache.
### Important

This connector expects records from Kafka to have a key and value that are stored as bytes or a string. If your data is already in Kafka in the format that you want in Redis consider using the ByteArrayConverter or the StringConverter for this connector. Keep in this does not need to be configured in the worker properties and can be configured at the connector level. If your data is not sitting in Kafka in the format you wish to persist in Redis consider using a Single Message Transformation to convert the data to a byte or string representation before it is written to Redis.
### Note

This connector supports deletes. If the record stored in Kafka has a null value, this connector will send a delete with the corresponding key to Redis.

## [Redis Geo Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sinks/RedisGeoSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisGeoSinkConnector
```

The Redis Geo Sink Connector is used to write data from Kafka to a Redis cache.
### Important

This connector expects data to be written in the following format. Key: `{"key":"cities", "member":"Austin"}`, Value: `{"latitude":30.2672, "longitude":97.7431}` 
### Note

This connector supports deletes. If the record stored in Kafka has a null value, this connector will send a delete with the corresponding key to Redis.

## [Redis Map Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sinks/RedisMapSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisMapSinkConnector
```

The Redis Geo Sink Connector is used to write data from Kafka to a Redis cache.
### Important

This connector expects data to be written in the following format. Key: `string or bytes`, Value: `{"latitude":"30.2672", "longitude":"97.7431"}` 
### Note

This connector supports deletes. If the record stored in Kafka has a null value, this connector will send a delete with the corresponding key to Redis.

## [Redis Cache Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sinks/RedisCacheSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisCacheSinkConnector
```

The Redis Cache Sink Connector is used to write data from Kafka to a Redis cache.
### Important

This connector expects records from Kafka to have a key and value that are stored as bytes or a string. If your data is already in Kafka in the format that you want in Redis consider using the ByteArrayConverter or the StringConverter for this connector. Keep in this does not need to be configured in the worker properties and can be configured at the connector level. If your data is not sitting in Kafka in the format you wish to persist in Redis consider using a Single Message Transformation to convert the data to a byte or string representation before it is written to Redis.
### Note

This connector supports deletes. If the record stored in Kafka has a null value, this connector will send a delete with the corresponding key to Redis.



# Development

## Building the source

```bash
mvn clean package
```

## Contributions

Contributions are always welcomed! Before you start any development please create an issue and
start a discussion. Create a pull request against your newly created issue and we're happy to see
if we can merge your pull request. First and foremost any time you're adding code to the code base
you need to include test coverage. Make sure that you run `mvn clean package` before submitting your
pull to ensure that all of the tests, checkstyle rules, and the package can be successfully built.