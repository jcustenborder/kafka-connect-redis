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

import com.github.jcustenborder.docker.junit5.CleanupMode;
import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.docker.junit5.DockerContainer;
import com.github.jcustenborder.docker.junit5.Port;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.docker.compose.connection.Container;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Compose(
    dockerComposePath = "src/test/resources/docker-compose.yml",
    cleanupMode = CleanupMode.AfterEach
)
public class RedisCacheSinkTaskReconnectIT {
  private static final Logger log = LoggerFactory.getLogger(RedisCacheSinkTaskReconnectIT.class);


  RedisCacheSinkTask task;

  @BeforeEach
  public void before() {
    this.task = new RedisCacheSinkTask();
  }

  @Test
  public void initialConnectionIssues(
      @DockerContainer(container = "redis") Container container,
      @Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException, IOException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of());
    this.task.initialize(context);
    container.stop();

    ExecutorService service = Executors.newSingleThreadExecutor();
    Future<?> future = service.submit(() -> task.start(
        ImmutableMap.of(RedisSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort())
        )
    ));
    container.start();
    Time.SYSTEM.sleep(2000);
    future.get();
  }

  void sendAndVerifyRecords(AbstractRedisSinkTask task, String topic, int keyIndex) throws ExecutionException, InterruptedException {
    final int count = 50;
    final Map<String, String> expected = new LinkedHashMap<>(count);
    final List<SinkRecord> records = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      int k = i + keyIndex;
      final String key = String.format("putWrite%s", k);
      final String value = String.format("This is value %s", i);
      records.add(
          write(topic,
              new SchemaAndValue(Schema.STRING_SCHEMA, key),
              new SchemaAndValue(Schema.STRING_SCHEMA, value)
          )
      );

      expected.put(key, value);
    }
    this.task.put(records);

    final byte[][] keys = expected.keySet().stream()
        .map(s -> s.getBytes(Charsets.UTF_8))
        .toArray(byte[][]::new);
    RedisFuture<List<KeyValue<byte[], byte[]>>> result = this.task.session.asyncCommands().mget(keys);
    List<KeyValue<byte[], byte[]>> actual = result.get();
    assertEquals(count, actual.size());
    for (KeyValue<byte[], byte[]> kv : actual) {
      final String key = new String(kv.getKey(), Charsets.UTF_8);
      final String value = new String(kv.getValue(), Charsets.UTF_8);
      assertEquals(value, expected.get(key), String.format("Value for key(%s) does not match.", key));
    }
  }


  @Test
  public void serverReset(
      @DockerContainer(container = "redis") Container container,
      @Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException, IOException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of());
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    sendAndVerifyRecords(task, topic, 0);
    container.stop();

    assertThrows(RetriableException.class, () -> {
      sendAndVerifyRecords(task, topic, 100);
    });
    container.start();
    sendAndVerifyRecords(task, topic, 100);
  }


  @AfterEach
  public void after() {
    if (null != this.task) {
      this.task.stop();
    }
  }

}
