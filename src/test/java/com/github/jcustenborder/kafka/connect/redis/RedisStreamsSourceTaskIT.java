/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.redis;

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.docker.junit5.Port;
import com.google.common.collect.ImmutableMap;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Compose(
    dockerComposePath = "src/test/resources/docker-compose.yml"
)
public class RedisStreamsSourceTaskIT {
  static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
      .field("ident", Schema.STRING_SCHEMA)
      .field("region", Schema.STRING_SCHEMA)
      .field("latitude", Schema.STRING_SCHEMA)
      .field("longitude", Schema.STRING_SCHEMA)
      .build();
  private static final Logger log = LoggerFactory.getLogger(RedisStreamsSourceTaskIT.class);
  RedisStreamsSourceTask task;

  @BeforeEach
  public void before() {
    this.task = new RedisStreamsSourceTask();
  }

  public SinkRecord structWrite(
      TestLocation location,
      String topic,
      int partition,
      AtomicLong offset
  ) {

    Struct value = new Struct(VALUE_SCHEMA)
        .put("ident", location.ident)
        .put("region", location.region)
        .put("latitude", Double.toString(location.latitude))
        .put("longitude", Double.toString(location.longitude));

    return new SinkRecord(topic, partition,
        Schema.STRING_SCHEMA, location.ident,
        value.schema(), value,
        offset.incrementAndGet());
  }

  Map<String, String> map(TestLocation location) {
    return ImmutableMap.of(
        "ident", location.ident,
        "region", location.region,
        "latitude", Double.toString(location.latitude),
        "longitude", Double.toString(location.longitude)
    );
  }

  @Test
  public void putWrite(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException, TimeoutException, IOException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SourceTaskContext context = mock(SourceTaskContext.class);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(
            RedisStreamsSourceConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()),
            RedisStreamsSourceConnectorConfig.REDIS_STREAMS_CONF, topic,
            RedisStreamsSourceConnectorConfig.REDIS_CONSUMER_GROUP_CONF, topic + "-group",
            RedisStreamsSourceConnectorConfig.REDIS_CONSUMER_ID_CONF, "0"
        )
    );
    final List<TestLocation> locations = TestLocation.loadLocations();
    final List<RedisFuture<String>> results = locations.stream()
        .map(this::map)
        .map(l -> this.task.session.asyncCommands().xadd(topic, l))
        .collect(Collectors.toList());
    LettuceFutures.awaitAll(30, TimeUnit.SECONDS, results.toArray(new RedisFuture[0]));
    List<SourceRecord> records = this.task.poll();
    assertNotNull(records);
    assertFalse(records.isEmpty());

    this.task.commit();
  }
//
//  void assertExists(List<TestLocation> locations) throws InterruptedException, ExecutionException, TimeoutException {
//    byte[][] fieldNames = VALUE_SCHEMA.fields().stream()
//        .map(f -> f.name().getBytes(Charsets.UTF_8))
//        .toArray(byte[][]::new);
//    for (TestLocation location : locations) {
//      byte[] key = location.ident.getBytes(Charsets.UTF_8);
//      Map<String, String> actual = this.task.session.asyncCommands().hmget(key, fieldNames)
//          .get(30, TimeUnit.SECONDS)
//          .stream()
//          .map(TestUtils::toString)
//          .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
//      Map<String, String> expected = ImmutableMap.of(
//          "ident", location.ident,
//          "region", location.region,
//          "latitude", Double.toString(location.latitude),
//          "longitude", Double.toString(location.longitude)
//      );
//      assertEquals(expected, actual);
//    }
//  }
//
//  void assertNotExists(List<TestLocation> locations) throws InterruptedException, ExecutionException, TimeoutException {
//    byte[][] keys = locations.stream()
//        .map(e -> e.ident.getBytes(Charsets.UTF_8))
//        .toArray(byte[][]::new);
//    final long written = this.task.session.asyncCommands().exists(keys).get();
//    assertEquals(0, written);
//  }

  @AfterEach
  public void after() {
    if (null != this.task) {
      this.task.stop();
    }
  }

}
