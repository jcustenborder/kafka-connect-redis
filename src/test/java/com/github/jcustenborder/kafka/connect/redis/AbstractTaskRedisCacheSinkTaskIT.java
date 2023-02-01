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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbstractTaskRedisCacheSinkTaskIT extends AbstractSinkTaskIntegrationTest<RedisCacheSinkTask> {
  private static final Logger log = LoggerFactory.getLogger(AbstractTaskRedisCacheSinkTaskIT.class);

  @Override
  protected RedisCacheSinkTask createTask() {
    return new RedisCacheSinkTask();
  }

  AtomicLong offset;

  @BeforeEach
  public void setupOffset() {
    this.offset = new AtomicLong(1L);
  }

  public SinkRecord structWrite(
      TestLocation location,
      String topic,
      int partition,
      AtomicLong offset
  ) {
    return new SinkRecord(topic, partition,
        Schema.STRING_SCHEMA, location.ident(),
        Schema.STRING_SCHEMA, location.region(),
        offset.incrementAndGet());
  }

  public SinkRecord structDelete(
      TestLocation location,
      String topic,
      int partition,
      AtomicLong offset
  ) {
    return new SinkRecord(topic, partition,
        Schema.STRING_SCHEMA, location.ident(),
        null, null,
        offset.incrementAndGet());
  }

  @Test
  public void emptyAssignment() throws ExecutionException, InterruptedException {
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of());
    this.task.start(this.settings);
  }

  Map<TopicPartition, Long> generateOffsets(int topics, int partitionBound) {
    Random random = new Random();
    Map<TopicPartition, Long> results = new LinkedHashMap<>();
    for (int i = 1; i < topics; i++) {
      String topicName = this.topic + i;
      int partitions = random.nextInt(partitionBound) + 1;
      for (int j = 0; j < partitions; j++) {
        int partition = random.nextInt(40) + 1;
        Long offset = Math.abs(random.nextLong());
        results.put(
            new TopicPartition(topicName, partition),
            offset
        );
      }
    }
    return results;
  }

  @Test
  public void existingMixedOffsets() throws ExecutionException, InterruptedException {
    Map<TopicPartition, Long> hmapBasedOffsets = generateOffsets(10, 40);
    Map<TopicPartition, Long> getBasedOffsets = new LinkedHashMap<>();
    getBasedOffsets.put(new TopicPartition("getBased", 1), 1234L);
    getBasedOffsets.put(new TopicPartition("getBased", 2), 4567L);
    getBasedOffsets.put(new TopicPartition("getBased", 3), 12344567L);

    //Override one of the get based offsets. This will be the case during migration.
    getBasedOffsets.put(
        hmapBasedOffsets.keySet().stream().findFirst().get(),
        1234L
    );


    try (StatefulRedisConnection<String, String> connection = this.connectionHelper.redisConnection()) {
      RedisCommands<String, String> commands = connection.sync();
      Map<String, Map<TopicPartition, Long>> topicOffsets = new LinkedHashMap<>();
      hmapBasedOffsets.forEach(((topicPartition, offsetAndMetadata) -> {
        Map<TopicPartition, Long> offsets = topicOffsets.computeIfAbsent(topicPartition.topic(), e -> new LinkedHashMap<>());
        offsets.put(topicPartition, offsetAndMetadata);
      }));

      topicOffsets.forEach((topic, offsets) -> {
        String topicKey = String.format("__kafka.offsets.%s", topic);
        Map<String, String> storedOffsets = offsets.entrySet().stream()
            .collect(Collectors.toMap(e -> Integer.toString(e.getKey().partition()), e -> Long.toString(e.getValue())));
        log.info("Calling hset('{}', {})", topicKey, storedOffsets);
        long value = commands.hset(topicKey, storedOffsets);
        log.info("result: {}", value);
      });

      getBasedOffsets.forEach(((topicPartition, offset) -> {
        String topicPartitionKey = String.format("__kafka.offset.%s.%s", topicPartition.topic(), topicPartition.partition());
        log.info("Calling set('{}', '{}')", topicPartitionKey, offset);
        commands.set(topicPartitionKey, Long.toString(offset));
      }));
    }

    Map<TopicPartition, Long> expectedOffsets = new LinkedHashMap<>();
    expectedOffsets.putAll(getBasedOffsets); //hmap is more important than get
    expectedOffsets.putAll(hmapBasedOffsets);
    when(this.sinkTaskContext.assignment()).thenReturn(expectedOffsets.keySet());

    doAnswer(invocationOnMock -> {
      Map<TopicPartition, Long> requestedOffsets = invocationOnMock.getArgument(0);
      log.info("requested offsets:\n{}", requestedOffsets);
      assertNotNull(requestedOffsets, "requested offsets cannot be null.");
      assertEquals(expectedOffsets, requestedOffsets);
      return null;
    }).when(this.sinkTaskContext).offset(anyMap());

    this.task.start(this.settings);

    verify(this.sinkTaskContext, times(1)).offset(anyMap());
    verify(this.sinkTaskContext, times(1)).assignment();
  }


  @Test
  public void existingOffsets() throws ExecutionException, InterruptedException {

    Map<TopicPartition, Long> expectedOffsets = generateOffsets(10, 40);
    when(this.sinkTaskContext.assignment()).thenReturn(expectedOffsets.keySet());
    log.info("Offsets {}", expectedOffsets);

    try (StatefulRedisConnection<String, String> connection = this.connectionHelper.redisConnection()) {
      RedisCommands<String, String> commands = connection.sync();
      Map<String, Map<TopicPartition, Long>> topicOffsets = new LinkedHashMap<>();
      expectedOffsets.forEach(((topicPartition, offsetAndMetadata) -> {
        Map<TopicPartition, Long> offsets = topicOffsets.computeIfAbsent(topicPartition.topic(), e -> new LinkedHashMap<>());
        offsets.put(topicPartition, offsetAndMetadata);
      }));

      topicOffsets.forEach((topic, offsets) -> {
        String topicKey = String.format("__kafka.offsets.%s", topic);
        Map<String, String> storedOffsets = offsets.entrySet().stream()
            .collect(Collectors.toMap(e -> Integer.toString(e.getKey().partition()), e -> Long.toString(e.getValue())));
        log.info("Calling hset('{}', {})", topicKey, storedOffsets);
        long value = commands.hset(topicKey, storedOffsets);
        log.info("result: {}", value);
      });
    }

//    expectedOffsets.put(new TopicPartition("WTF", 1), 1234123L);
    doAnswer(invocationOnMock -> {
      Map<TopicPartition, Long> requestedOffsets = invocationOnMock.getArgument(0);
      log.info("requested offsets:\n{}", requestedOffsets);
      assertNotNull(requestedOffsets, "requested offsets cannot be null.");
      assertEquals(expectedOffsets, requestedOffsets);
      return null;
    }).when(this.sinkTaskContext).offset(anyMap());
    this.task.start(this.settings);
  }


  @Test
  public void putEmpty() throws ExecutionException, InterruptedException {
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(this.settings);
    this.task.put(ImmutableList.of());
  }

  @Test
  public void putWrite() throws ExecutionException, InterruptedException, IOException, TimeoutException {
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(this.settings);

    final List<TestLocation> locations = TestLocation.loadLocations();
    final AtomicLong offset = new AtomicLong(1L);
    final List<SinkRecord> writes = locations.stream()
        .map(l -> structWrite(l, this.topic, 1, offset))
        .collect(Collectors.toList());
    final Map<TopicPartition, OffsetAndMetadata> offsets = offsets(writes);
    this.task.put(writes);
    this.task.flush(offsets);

    byte[][] keys = locations.stream()
        .map(l -> l.ident.getBytes(Charsets.UTF_8))
        .toArray(byte[][]::new);


    RedisFuture<byte[]>[] futures = new RedisFuture[keys.length];
    final Map<String, String> results = new ConcurrentHashMap<>(futures.length);

    for (int i = 0; i < keys.length; i++) {
      final byte[] key = keys[i];
      futures[i] = this.task.session.asyncCommands().get(key);
      futures[i].thenAccept(value -> results.put(
          new String(key, StandardCharsets.UTF_8),
          new String(value, StandardCharsets.UTF_8)
      ));
    }
    this.task.session.connection().flushCommands();
    LettuceFutures.awaitAll(30, TimeUnit.SECONDS, futures);
    assertEquals(keys.length, results.size());


    results.forEach((key, value) -> {
      Optional<TestLocation> location = locations.stream()
          .filter(l -> l.ident.equals(key))
          .findAny();
      assertTrue(location.isPresent(), "location should have existed.");
      assertEquals(location.get().region, value);
    });
  }




  void assertOffsets(Map<TopicPartition, OffsetAndMetadata> expectedOffsets) {
    Map<String, Map<TopicPartition, Long>> topics = new LinkedHashMap<>();
    expectedOffsets.forEach(((topicPartition, offsetAndMetadata) -> {
      Map<TopicPartition, Long> partitions = topics.computeIfAbsent(topicPartition.topic(), a -> new LinkedHashMap<>());
      partitions.put(topicPartition, offsetAndMetadata.offset());
    }));

    topics.forEach((topic, partitions) -> {
      String topicKey = String.format("__kafka.offsets.%s", topic);
      byte[] key = topicKey.getBytes(StandardCharsets.UTF_8);

      log.info("Calling hgetall('{}')", topicKey);
      RedisFuture<Map<byte[], byte[]>> future = this.task.session.asyncCommands().hgetall(key);
      this.task.session.connection().flushCommands();
      try {
        Map<byte[], byte[]> storedOffsets = future.get(30, TimeUnit.SECONDS);
        Map<String, String> actualOffsets = storedOffsets.entrySet().stream()
            .map(e -> new AbstractMap.SimpleEntry<>(
                new String(e.getKey()),
                new String(e.getValue()))
            ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        log.info("actualOffsets = {}", actualOffsets);
        Map<String, String> expectedStringOffsets = expectedOffsets.entrySet().stream()
            .map(e -> new AbstractMap.SimpleEntry<>(
                    Integer.toString(e.getKey().partition()),
                    Long.toString(e.getValue().offset())
                )
            ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(expectedStringOffsets, actualOffsets, "Stored offsets do not match");

      } catch (Exception ex) {
        fail(ex);
      }

    });


  }


  @Test
  public void putDelete() throws ExecutionException, InterruptedException, IOException, TimeoutException {
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(this.settings);

    final List<TestLocation> locations = TestLocation.loadLocations();
    final AtomicLong offset = new AtomicLong(1L);
    final List<SinkRecord> writes = locations.stream()
        .map(l -> structWrite(l, topic, 1, offset))
        .collect(Collectors.toList());
    this.task.put(writes);
    Map<TopicPartition, OffsetAndMetadata> offsets = offsets(writes);
    this.task.flush(offsets);
    assertOffsets(offsets);

    log.info("Check that the keys have been written.");
    byte[][] keys = locations.stream()
        .map(l -> l.ident.getBytes(Charsets.UTF_8))
        .toArray(byte[][]::new);

    RedisFuture<Long>[] futures = new RedisFuture[keys.length];
    for (int i = 0; i < futures.length; i++) {
      futures[i] = this.task.session.asyncCommands().exists(keys[i]);
    }
    this.task.session.connection().flushCommands();
    LettuceFutures.awaitAll(
        30000,
        TimeUnit.MILLISECONDS,
        futures
    );

    long count = 0;
    for (RedisFuture<Long> future : futures) {
      count += future.get();
    }
    assertEquals(locations.size(), count, "Count written does not match.");


    final List<SinkRecord> deletes = locations.stream()
        .map(l -> structDelete(l, topic, 1, offset))
        .collect(Collectors.toList());

    this.task.put(deletes);
    offsets = offsets(deletes);
    this.task.flush(offsets);
    assertOffsets(offsets);

    futures = new RedisFuture[keys.length];
    for (int i = 0; i < futures.length; i++) {
      futures[i] = this.task.session.asyncCommands().exists(keys[i]);
    }
    this.task.session.connection().flushCommands();
    LettuceFutures.awaitAll(
        30000,
        TimeUnit.MILLISECONDS,
        futures
    );

    count = 0;
    for (RedisFuture<Long> future : futures) {
      count += future.get();
    }
    assertEquals(0, count, "Count written does not match.");

  }

  @AfterEach
  public void after() {
    if (null != this.task) {
      this.task.stop();
    }
  }

}
