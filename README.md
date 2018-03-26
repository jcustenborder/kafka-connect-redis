
# Introduction


# Sink Connectors


## RedisSinkConnector

Sink connector for writing data to Redis



### Important

This connector expects to received data with a key of bytes and a values of bytes. If your data is structured you need to use a Transformation to convert this data from structured data like a Struct to an array of bytes for the key and value.

### Note

This connector supports deletes. It will issue a delete to the Redis cluster for any key that does not have a corresponding value. In Kafka a record that contains a key and a null value is considered a delete.


### Configuration

##### `redis.hosts`
*Importance:* High

*Type:* List

*Default Value:* [localhost:6379]



##### `redis.database`
*Importance:* Medium

*Type:* Int

*Default Value:* 1



##### `redis.operation.timeout.ms`
*Importance:* Medium

*Type:* Long

*Default Value:* 10000

*Validator:* [100,...]


redis.operation.timeout.ms
##### `redis.password`
*Importance:* Medium

*Type:* Password

*Default Value:* [hidden]



##### `redis.ssl.enabled`
*Importance:* Medium

*Type:* Boolean

*Default Value:* false




#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=RedisSinkConnector1
connector.class=com.github.jcustenborder.kafka.connect.redis.RedisSinkConnector
tasks.max=1
topics=< Required Configuration >
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "config" : {
    "name" : "RedisSinkConnector1",
    "connector.class" : "com.github.jcustenborder.kafka.connect.redis.RedisSinkConnector",
    "tasks.max" : "1",
    "topics" : "< Required Configuration >"
  }
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



