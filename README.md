# Introduction
[Documentation](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis)

Installation through the [Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html)

The Redis plugin is a collection of connectors that are used to interact with a Redis cluster.



# Sink Connectors
## [Redis Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sinks/RedisSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisSinkConnector
```

The Redis Sink Connector is used to write data from Kafka to a Redis cache.
### Important

This connector expects records from Kafka to have a key and value that are stored as bytes or a string. If your data is already in Kafka in the format that you want in Redis consider using the ByteArrayConverter or the StringConverter for this connector. Keep in this does not need to be configured in the worker properties and can be configured at the connector level. If your data is not sitting in Kafka in the format you wish to persist in Redis consider using a Single Message Transformation to convert the data to a byte or string representation before it is written to Redis.
### Note

This connector supports deletes. If the record stored in Kafka has a null value, this connector will send a delete with the corresponding key to Redis.
### Configuration

#### General


##### `redis.hosts`

The Redis hosts to connect to.

*Importance:* HIGH

*Type:* LIST

*Default Value:* [localhost:6379]



##### `redis.client.mode`

The client mode to use when interacting with the Redis cluster.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* Standalone

*Validator:* Matches: ``Standalone``, ``Cluster``



##### `redis.connection.attempts`

The number of attempt when connecting to redis.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 3

*Validator:* [1,...]



##### `redis.connection.retry.delay.ms`

The amount of milliseconds to wait between redis connection attempts.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 2000

*Validator:* [100,...]



##### `redis.database`

Redis database to connect to.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 1



##### `redis.operation.timeout.ms`

The amount of time in milliseconds before an operation is marked as timed out.

*Importance:* MEDIUM

*Type:* LONG

*Default Value:* 10000

*Validator:* [100,...]



##### `redis.password`

Password used to connect to Redis.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.enabled`

Flag to determine if SSL is enabled.

*Importance:* MEDIUM

*Type:* BOOLEAN



##### `redis.ssl.keystore.password`

The password for the SSL keystore.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.keystore.path`

The path to the SSL keystore.

*Importance:* MEDIUM

*Type:* STRING



##### `redis.ssl.truststore.password`

The password for the SSL truststore.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.truststore.path`

The path to the SSL truststore.

*Importance:* MEDIUM

*Type:* STRING



##### `redis.auto.reconnect.enabled`

Flag to determine if the Redis client should automatically reconnect.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true



##### `redis.charset`

The character set to use for String key and values.

*Importance:* LOW

*Type:* STRING

*Default Value:* UTF-8

*Validator:* Valid values: 'Big5', 'Big5-HKSCS', 'CESU-8', 'EUC-JP', 'EUC-KR', 'GB18030', 'GB2312', 'GBK', 'IBM-Thai', 'IBM00858', 'IBM01140', 'IBM01141', 'IBM01142', 'IBM01143', 'IBM01144', 'IBM01145', 'IBM01146', 'IBM01147', 'IBM01148', 'IBM01149', 'IBM037', 'IBM1026', 'IBM1047', 'IBM273', 'IBM277', 'IBM278', 'IBM280', 'IBM284', 'IBM285', 'IBM290', 'IBM297', 'IBM420', 'IBM424', 'IBM437', 'IBM500', 'IBM775', 'IBM850', 'IBM852', 'IBM855', 'IBM857', 'IBM860', 'IBM861', 'IBM862', 'IBM863', 'IBM864', 'IBM865', 'IBM866', 'IBM868', 'IBM869', 'IBM870', 'IBM871', 'IBM918', 'ISO-2022-CN', 'ISO-2022-JP', 'ISO-2022-JP-2', 'ISO-2022-KR', 'ISO-8859-1', 'ISO-8859-13', 'ISO-8859-15', 'ISO-8859-2', 'ISO-8859-3', 'ISO-8859-4', 'ISO-8859-5', 'ISO-8859-6', 'ISO-8859-7', 'ISO-8859-8', 'ISO-8859-9', 'JIS_X0201', 'JIS_X0212-1990', 'KOI8-R', 'KOI8-U', 'Shift_JIS', 'TIS-620', 'US-ASCII', 'UTF-16', 'UTF-16BE', 'UTF-16LE', 'UTF-32', 'UTF-32BE', 'UTF-32LE', 'UTF-8', 'X-UTF-32BE-BOM', 'X-UTF-32LE-BOM', 'windows-1250', 'windows-1251', 'windows-1252', 'windows-1253', 'windows-1254', 'windows-1255', 'windows-1256', 'windows-1257', 'windows-1258', 'windows-31j', 'x-Big5-HKSCS-2001', 'x-Big5-Solaris', 'x-COMPOUND_TEXT', 'x-EUC-TW', 'x-IBM1006', 'x-IBM1025', 'x-IBM1046', 'x-IBM1097', 'x-IBM1098', 'x-IBM1112', 'x-IBM1122', 'x-IBM1123', 'x-IBM1124', 'x-IBM1166', 'x-IBM1364', 'x-IBM1381', 'x-IBM1383', 'x-IBM300', 'x-IBM33722', 'x-IBM737', 'x-IBM833', 'x-IBM834', 'x-IBM856', 'x-IBM874', 'x-IBM875', 'x-IBM921', 'x-IBM922', 'x-IBM930', 'x-IBM933', 'x-IBM935', 'x-IBM937', 'x-IBM939', 'x-IBM942', 'x-IBM942C', 'x-IBM943', 'x-IBM943C', 'x-IBM948', 'x-IBM949', 'x-IBM949C', 'x-IBM950', 'x-IBM964', 'x-IBM970', 'x-ISCII91', 'x-ISO-2022-CN-CNS', 'x-ISO-2022-CN-GB', 'x-JIS0208', 'x-JISAutoDetect', 'x-Johab', 'x-MS932_0213', 'x-MS950-HKSCS', 'x-MS950-HKSCS-XP', 'x-MacArabic', 'x-MacCentralEurope', 'x-MacCroatian', 'x-MacCyrillic', 'x-MacDingbat', 'x-MacGreek', 'x-MacHebrew', 'x-MacIceland', 'x-MacRoman', 'x-MacRomania', 'x-MacSymbol', 'x-MacThai', 'x-MacTurkish', 'x-MacUkraine', 'x-PCK', 'x-SJIS_0213', 'x-UTF-16LE-BOM', 'x-euc-jp-linux', 'x-eucJP-Open', 'x-iso-8859-11', 'x-mswin-936', 'x-windows-50220', 'x-windows-50221', 'x-windows-874', 'x-windows-949', 'x-windows-950', 'x-windows-iso2022jp'



##### `redis.request.queue.size`

The maximum number of queued requests to Redis.

*Importance:* LOW

*Type:* INT

*Default Value:* 2147483647



##### `redis.socket.connect.timeout.ms`

The amount of time in milliseconds to wait before timing out a socket when connecting.

*Importance:* LOW

*Type:* INT

*Default Value:* 10000



##### `redis.socket.keep.alive.enabled`

Flag to enable a keepalive to Redis.

*Importance:* LOW

*Type:* BOOLEAN



##### `redis.socket.tcp.no.delay.enabled`

Flag to enable TCP no delay should be used.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true



##### `redis.ssl.provider`

The SSL provider to use.

*Importance:* LOW

*Type:* STRING

*Default Value:* JDK

*Validator:* Matches: ``OPENSSL``, ``JDK``



## [Redis Geo Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sinks/RedisGeoSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisGeoSinkConnector
```

The Redis Geo Sink Connector is used to write data from Kafka to a Redis cache.
### Important

This connector expects data to be written in the following format. Key: `{"key":"cities", "member":"Austin"}`, Value: `{"latitude":30.2672, "longitude":97.7431}` 
### Note

This connector supports deletes. If the record stored in Kafka has a null value, this connector will send a delete with the corresponding key to Redis.
### Configuration

#### General


##### `redis.hosts`

The Redis hosts to connect to.

*Importance:* HIGH

*Type:* LIST

*Default Value:* [localhost:6379]



##### `redis.client.mode`

The client mode to use when interacting with the Redis cluster.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* Standalone

*Validator:* Matches: ``Standalone``, ``Cluster``



##### `redis.connection.attempts`

The number of attempt when connecting to redis.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 3

*Validator:* [1,...]



##### `redis.connection.retry.delay.ms`

The amount of milliseconds to wait between redis connection attempts.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 2000

*Validator:* [100,...]



##### `redis.database`

Redis database to connect to.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 1



##### `redis.operation.timeout.ms`

The amount of time in milliseconds before an operation is marked as timed out.

*Importance:* MEDIUM

*Type:* LONG

*Default Value:* 10000

*Validator:* [100,...]



##### `redis.password`

Password used to connect to Redis.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.enabled`

Flag to determine if SSL is enabled.

*Importance:* MEDIUM

*Type:* BOOLEAN



##### `redis.ssl.keystore.password`

The password for the SSL keystore.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.keystore.path`

The path to the SSL keystore.

*Importance:* MEDIUM

*Type:* STRING



##### `redis.ssl.truststore.password`

The password for the SSL truststore.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.truststore.path`

The path to the SSL truststore.

*Importance:* MEDIUM

*Type:* STRING



##### `redis.auto.reconnect.enabled`

Flag to determine if the Redis client should automatically reconnect.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true



##### `redis.charset`

The character set to use for String key and values.

*Importance:* LOW

*Type:* STRING

*Default Value:* UTF-8

*Validator:* Valid values: 'Big5', 'Big5-HKSCS', 'CESU-8', 'EUC-JP', 'EUC-KR', 'GB18030', 'GB2312', 'GBK', 'IBM-Thai', 'IBM00858', 'IBM01140', 'IBM01141', 'IBM01142', 'IBM01143', 'IBM01144', 'IBM01145', 'IBM01146', 'IBM01147', 'IBM01148', 'IBM01149', 'IBM037', 'IBM1026', 'IBM1047', 'IBM273', 'IBM277', 'IBM278', 'IBM280', 'IBM284', 'IBM285', 'IBM290', 'IBM297', 'IBM420', 'IBM424', 'IBM437', 'IBM500', 'IBM775', 'IBM850', 'IBM852', 'IBM855', 'IBM857', 'IBM860', 'IBM861', 'IBM862', 'IBM863', 'IBM864', 'IBM865', 'IBM866', 'IBM868', 'IBM869', 'IBM870', 'IBM871', 'IBM918', 'ISO-2022-CN', 'ISO-2022-JP', 'ISO-2022-JP-2', 'ISO-2022-KR', 'ISO-8859-1', 'ISO-8859-13', 'ISO-8859-15', 'ISO-8859-2', 'ISO-8859-3', 'ISO-8859-4', 'ISO-8859-5', 'ISO-8859-6', 'ISO-8859-7', 'ISO-8859-8', 'ISO-8859-9', 'JIS_X0201', 'JIS_X0212-1990', 'KOI8-R', 'KOI8-U', 'Shift_JIS', 'TIS-620', 'US-ASCII', 'UTF-16', 'UTF-16BE', 'UTF-16LE', 'UTF-32', 'UTF-32BE', 'UTF-32LE', 'UTF-8', 'X-UTF-32BE-BOM', 'X-UTF-32LE-BOM', 'windows-1250', 'windows-1251', 'windows-1252', 'windows-1253', 'windows-1254', 'windows-1255', 'windows-1256', 'windows-1257', 'windows-1258', 'windows-31j', 'x-Big5-HKSCS-2001', 'x-Big5-Solaris', 'x-COMPOUND_TEXT', 'x-EUC-TW', 'x-IBM1006', 'x-IBM1025', 'x-IBM1046', 'x-IBM1097', 'x-IBM1098', 'x-IBM1112', 'x-IBM1122', 'x-IBM1123', 'x-IBM1124', 'x-IBM1166', 'x-IBM1364', 'x-IBM1381', 'x-IBM1383', 'x-IBM300', 'x-IBM33722', 'x-IBM737', 'x-IBM833', 'x-IBM834', 'x-IBM856', 'x-IBM874', 'x-IBM875', 'x-IBM921', 'x-IBM922', 'x-IBM930', 'x-IBM933', 'x-IBM935', 'x-IBM937', 'x-IBM939', 'x-IBM942', 'x-IBM942C', 'x-IBM943', 'x-IBM943C', 'x-IBM948', 'x-IBM949', 'x-IBM949C', 'x-IBM950', 'x-IBM964', 'x-IBM970', 'x-ISCII91', 'x-ISO-2022-CN-CNS', 'x-ISO-2022-CN-GB', 'x-JIS0208', 'x-JISAutoDetect', 'x-Johab', 'x-MS932_0213', 'x-MS950-HKSCS', 'x-MS950-HKSCS-XP', 'x-MacArabic', 'x-MacCentralEurope', 'x-MacCroatian', 'x-MacCyrillic', 'x-MacDingbat', 'x-MacGreek', 'x-MacHebrew', 'x-MacIceland', 'x-MacRoman', 'x-MacRomania', 'x-MacSymbol', 'x-MacThai', 'x-MacTurkish', 'x-MacUkraine', 'x-PCK', 'x-SJIS_0213', 'x-UTF-16LE-BOM', 'x-euc-jp-linux', 'x-eucJP-Open', 'x-iso-8859-11', 'x-mswin-936', 'x-windows-50220', 'x-windows-50221', 'x-windows-874', 'x-windows-949', 'x-windows-950', 'x-windows-iso2022jp'



##### `redis.request.queue.size`

The maximum number of queued requests to Redis.

*Importance:* LOW

*Type:* INT

*Default Value:* 2147483647



##### `redis.socket.connect.timeout.ms`

The amount of time in milliseconds to wait before timing out a socket when connecting.

*Importance:* LOW

*Type:* INT

*Default Value:* 10000



##### `redis.socket.keep.alive.enabled`

Flag to enable a keepalive to Redis.

*Importance:* LOW

*Type:* BOOLEAN



##### `redis.socket.tcp.no.delay.enabled`

Flag to enable TCP no delay should be used.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true



##### `redis.ssl.provider`

The SSL provider to use.

*Importance:* LOW

*Type:* STRING

*Default Value:* JDK

*Validator:* Matches: ``OPENSSL``, ``JDK``



## [Redis Map Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sinks/RedisMapSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisMapSinkConnector
```

The Redis Geo Sink Connector is used to write data from Kafka to a Redis cache.
### Important

This connector expects data to be written in the following format. Key: `string or bytes`, Value: `{"latitude":"30.2672", "longitude":"97.7431"}` 
### Note

This connector supports deletes. If the record stored in Kafka has a null value, this connector will send a delete with the corresponding key to Redis.
### Configuration

#### General


##### `redis.hosts`

The Redis hosts to connect to.

*Importance:* HIGH

*Type:* LIST

*Default Value:* [localhost:6379]



##### `redis.client.mode`

The client mode to use when interacting with the Redis cluster.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* Standalone

*Validator:* Matches: ``Standalone``, ``Cluster``



##### `redis.connection.attempts`

The number of attempt when connecting to redis.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 3

*Validator:* [1,...]



##### `redis.connection.retry.delay.ms`

The amount of milliseconds to wait between redis connection attempts.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 2000

*Validator:* [100,...]



##### `redis.database`

Redis database to connect to.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 1



##### `redis.operation.timeout.ms`

The amount of time in milliseconds before an operation is marked as timed out.

*Importance:* MEDIUM

*Type:* LONG

*Default Value:* 10000

*Validator:* [100,...]



##### `redis.password`

Password used to connect to Redis.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.enabled`

Flag to determine if SSL is enabled.

*Importance:* MEDIUM

*Type:* BOOLEAN



##### `redis.ssl.keystore.password`

The password for the SSL keystore.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.keystore.path`

The path to the SSL keystore.

*Importance:* MEDIUM

*Type:* STRING



##### `redis.ssl.truststore.password`

The password for the SSL truststore.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.truststore.path`

The path to the SSL truststore.

*Importance:* MEDIUM

*Type:* STRING



##### `redis.auto.reconnect.enabled`

Flag to determine if the Redis client should automatically reconnect.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true



##### `redis.charset`

The character set to use for String key and values.

*Importance:* LOW

*Type:* STRING

*Default Value:* UTF-8

*Validator:* Valid values: 'Big5', 'Big5-HKSCS', 'CESU-8', 'EUC-JP', 'EUC-KR', 'GB18030', 'GB2312', 'GBK', 'IBM-Thai', 'IBM00858', 'IBM01140', 'IBM01141', 'IBM01142', 'IBM01143', 'IBM01144', 'IBM01145', 'IBM01146', 'IBM01147', 'IBM01148', 'IBM01149', 'IBM037', 'IBM1026', 'IBM1047', 'IBM273', 'IBM277', 'IBM278', 'IBM280', 'IBM284', 'IBM285', 'IBM290', 'IBM297', 'IBM420', 'IBM424', 'IBM437', 'IBM500', 'IBM775', 'IBM850', 'IBM852', 'IBM855', 'IBM857', 'IBM860', 'IBM861', 'IBM862', 'IBM863', 'IBM864', 'IBM865', 'IBM866', 'IBM868', 'IBM869', 'IBM870', 'IBM871', 'IBM918', 'ISO-2022-CN', 'ISO-2022-JP', 'ISO-2022-JP-2', 'ISO-2022-KR', 'ISO-8859-1', 'ISO-8859-13', 'ISO-8859-15', 'ISO-8859-2', 'ISO-8859-3', 'ISO-8859-4', 'ISO-8859-5', 'ISO-8859-6', 'ISO-8859-7', 'ISO-8859-8', 'ISO-8859-9', 'JIS_X0201', 'JIS_X0212-1990', 'KOI8-R', 'KOI8-U', 'Shift_JIS', 'TIS-620', 'US-ASCII', 'UTF-16', 'UTF-16BE', 'UTF-16LE', 'UTF-32', 'UTF-32BE', 'UTF-32LE', 'UTF-8', 'X-UTF-32BE-BOM', 'X-UTF-32LE-BOM', 'windows-1250', 'windows-1251', 'windows-1252', 'windows-1253', 'windows-1254', 'windows-1255', 'windows-1256', 'windows-1257', 'windows-1258', 'windows-31j', 'x-Big5-HKSCS-2001', 'x-Big5-Solaris', 'x-COMPOUND_TEXT', 'x-EUC-TW', 'x-IBM1006', 'x-IBM1025', 'x-IBM1046', 'x-IBM1097', 'x-IBM1098', 'x-IBM1112', 'x-IBM1122', 'x-IBM1123', 'x-IBM1124', 'x-IBM1166', 'x-IBM1364', 'x-IBM1381', 'x-IBM1383', 'x-IBM300', 'x-IBM33722', 'x-IBM737', 'x-IBM833', 'x-IBM834', 'x-IBM856', 'x-IBM874', 'x-IBM875', 'x-IBM921', 'x-IBM922', 'x-IBM930', 'x-IBM933', 'x-IBM935', 'x-IBM937', 'x-IBM939', 'x-IBM942', 'x-IBM942C', 'x-IBM943', 'x-IBM943C', 'x-IBM948', 'x-IBM949', 'x-IBM949C', 'x-IBM950', 'x-IBM964', 'x-IBM970', 'x-ISCII91', 'x-ISO-2022-CN-CNS', 'x-ISO-2022-CN-GB', 'x-JIS0208', 'x-JISAutoDetect', 'x-Johab', 'x-MS932_0213', 'x-MS950-HKSCS', 'x-MS950-HKSCS-XP', 'x-MacArabic', 'x-MacCentralEurope', 'x-MacCroatian', 'x-MacCyrillic', 'x-MacDingbat', 'x-MacGreek', 'x-MacHebrew', 'x-MacIceland', 'x-MacRoman', 'x-MacRomania', 'x-MacSymbol', 'x-MacThai', 'x-MacTurkish', 'x-MacUkraine', 'x-PCK', 'x-SJIS_0213', 'x-UTF-16LE-BOM', 'x-euc-jp-linux', 'x-eucJP-Open', 'x-iso-8859-11', 'x-mswin-936', 'x-windows-50220', 'x-windows-50221', 'x-windows-874', 'x-windows-949', 'x-windows-950', 'x-windows-iso2022jp'



##### `redis.request.queue.size`

The maximum number of queued requests to Redis.

*Importance:* LOW

*Type:* INT

*Default Value:* 2147483647



##### `redis.socket.connect.timeout.ms`

The amount of time in milliseconds to wait before timing out a socket when connecting.

*Importance:* LOW

*Type:* INT

*Default Value:* 10000



##### `redis.socket.keep.alive.enabled`

Flag to enable a keepalive to Redis.

*Importance:* LOW

*Type:* BOOLEAN



##### `redis.socket.tcp.no.delay.enabled`

Flag to enable TCP no delay should be used.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true



##### `redis.ssl.provider`

The SSL provider to use.

*Importance:* LOW

*Type:* STRING

*Default Value:* JDK

*Validator:* Matches: ``OPENSSL``, ``JDK``



## [Redis Cache Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-redis/sinks/RedisCacheSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.redis.RedisCacheSinkConnector
```

The Redis Cache Sink Connector is used to write data from Kafka to a Redis cache.
### Important

This connector expects records from Kafka to have a key and value that are stored as bytes or a string. If your data is already in Kafka in the format that you want in Redis consider using the ByteArrayConverter or the StringConverter for this connector. Keep in this does not need to be configured in the worker properties and can be configured at the connector level. If your data is not sitting in Kafka in the format you wish to persist in Redis consider using a Single Message Transformation to convert the data to a byte or string representation before it is written to Redis.
### Note

This connector supports deletes. If the record stored in Kafka has a null value, this connector will send a delete with the corresponding key to Redis.
### Configuration

#### General


##### `redis.hosts`

The Redis hosts to connect to.

*Importance:* HIGH

*Type:* LIST

*Default Value:* [localhost:6379]



##### `redis.client.mode`

The client mode to use when interacting with the Redis cluster.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* Standalone

*Validator:* Matches: ``Standalone``, ``Cluster``



##### `redis.connection.attempts`

The number of attempt when connecting to redis.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 3

*Validator:* [1,...]



##### `redis.connection.retry.delay.ms`

The amount of milliseconds to wait between redis connection attempts.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 2000

*Validator:* [100,...]



##### `redis.database`

Redis database to connect to.

*Importance:* MEDIUM

*Type:* INT

*Default Value:* 1



##### `redis.operation.timeout.ms`

The amount of time in milliseconds before an operation is marked as timed out.

*Importance:* MEDIUM

*Type:* LONG

*Default Value:* 10000

*Validator:* [100,...]



##### `redis.password`

Password used to connect to Redis.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.enabled`

Flag to determine if SSL is enabled.

*Importance:* MEDIUM

*Type:* BOOLEAN



##### `redis.ssl.keystore.password`

The password for the SSL keystore.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.keystore.path`

The path to the SSL keystore.

*Importance:* MEDIUM

*Type:* STRING



##### `redis.ssl.truststore.password`

The password for the SSL truststore.

*Importance:* MEDIUM

*Type:* PASSWORD

*Default Value:* [hidden]



##### `redis.ssl.truststore.path`

The path to the SSL truststore.

*Importance:* MEDIUM

*Type:* STRING



##### `redis.auto.reconnect.enabled`

Flag to determine if the Redis client should automatically reconnect.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true



##### `redis.charset`

The character set to use for String key and values.

*Importance:* LOW

*Type:* STRING

*Default Value:* UTF-8

*Validator:* Valid values: 'Big5', 'Big5-HKSCS', 'CESU-8', 'EUC-JP', 'EUC-KR', 'GB18030', 'GB2312', 'GBK', 'IBM-Thai', 'IBM00858', 'IBM01140', 'IBM01141', 'IBM01142', 'IBM01143', 'IBM01144', 'IBM01145', 'IBM01146', 'IBM01147', 'IBM01148', 'IBM01149', 'IBM037', 'IBM1026', 'IBM1047', 'IBM273', 'IBM277', 'IBM278', 'IBM280', 'IBM284', 'IBM285', 'IBM290', 'IBM297', 'IBM420', 'IBM424', 'IBM437', 'IBM500', 'IBM775', 'IBM850', 'IBM852', 'IBM855', 'IBM857', 'IBM860', 'IBM861', 'IBM862', 'IBM863', 'IBM864', 'IBM865', 'IBM866', 'IBM868', 'IBM869', 'IBM870', 'IBM871', 'IBM918', 'ISO-2022-CN', 'ISO-2022-JP', 'ISO-2022-JP-2', 'ISO-2022-KR', 'ISO-8859-1', 'ISO-8859-13', 'ISO-8859-15', 'ISO-8859-2', 'ISO-8859-3', 'ISO-8859-4', 'ISO-8859-5', 'ISO-8859-6', 'ISO-8859-7', 'ISO-8859-8', 'ISO-8859-9', 'JIS_X0201', 'JIS_X0212-1990', 'KOI8-R', 'KOI8-U', 'Shift_JIS', 'TIS-620', 'US-ASCII', 'UTF-16', 'UTF-16BE', 'UTF-16LE', 'UTF-32', 'UTF-32BE', 'UTF-32LE', 'UTF-8', 'X-UTF-32BE-BOM', 'X-UTF-32LE-BOM', 'windows-1250', 'windows-1251', 'windows-1252', 'windows-1253', 'windows-1254', 'windows-1255', 'windows-1256', 'windows-1257', 'windows-1258', 'windows-31j', 'x-Big5-HKSCS-2001', 'x-Big5-Solaris', 'x-COMPOUND_TEXT', 'x-EUC-TW', 'x-IBM1006', 'x-IBM1025', 'x-IBM1046', 'x-IBM1097', 'x-IBM1098', 'x-IBM1112', 'x-IBM1122', 'x-IBM1123', 'x-IBM1124', 'x-IBM1166', 'x-IBM1364', 'x-IBM1381', 'x-IBM1383', 'x-IBM300', 'x-IBM33722', 'x-IBM737', 'x-IBM833', 'x-IBM834', 'x-IBM856', 'x-IBM874', 'x-IBM875', 'x-IBM921', 'x-IBM922', 'x-IBM930', 'x-IBM933', 'x-IBM935', 'x-IBM937', 'x-IBM939', 'x-IBM942', 'x-IBM942C', 'x-IBM943', 'x-IBM943C', 'x-IBM948', 'x-IBM949', 'x-IBM949C', 'x-IBM950', 'x-IBM964', 'x-IBM970', 'x-ISCII91', 'x-ISO-2022-CN-CNS', 'x-ISO-2022-CN-GB', 'x-JIS0208', 'x-JISAutoDetect', 'x-Johab', 'x-MS932_0213', 'x-MS950-HKSCS', 'x-MS950-HKSCS-XP', 'x-MacArabic', 'x-MacCentralEurope', 'x-MacCroatian', 'x-MacCyrillic', 'x-MacDingbat', 'x-MacGreek', 'x-MacHebrew', 'x-MacIceland', 'x-MacRoman', 'x-MacRomania', 'x-MacSymbol', 'x-MacThai', 'x-MacTurkish', 'x-MacUkraine', 'x-PCK', 'x-SJIS_0213', 'x-UTF-16LE-BOM', 'x-euc-jp-linux', 'x-eucJP-Open', 'x-iso-8859-11', 'x-mswin-936', 'x-windows-50220', 'x-windows-50221', 'x-windows-874', 'x-windows-949', 'x-windows-950', 'x-windows-iso2022jp'



##### `redis.request.queue.size`

The maximum number of queued requests to Redis.

*Importance:* LOW

*Type:* INT

*Default Value:* 2147483647



##### `redis.socket.connect.timeout.ms`

The amount of time in milliseconds to wait before timing out a socket when connecting.

*Importance:* LOW

*Type:* INT

*Default Value:* 10000



##### `redis.socket.keep.alive.enabled`

Flag to enable a keepalive to Redis.

*Importance:* LOW

*Type:* BOOLEAN



##### `redis.socket.tcp.no.delay.enabled`

Flag to enable TCP no delay should be used.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true



##### `redis.ssl.provider`

The SSL provider to use.

*Importance:* LOW

*Type:* STRING

*Default Value:* JDK

*Validator:* Matches: ``OPENSSL``, ``JDK``





# Development

## Building the source

```bash
mvn clean package
```