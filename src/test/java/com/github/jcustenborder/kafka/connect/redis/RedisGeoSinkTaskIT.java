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
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import io.lettuce.core.GeoCoordinates;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Compose(
    dockerComposePath = "src/test/resources/docker-compose.yml"
)
public class RedisGeoSinkTaskIT {
  private static final Logger log = LoggerFactory.getLogger(RedisGeoSinkTaskIT.class);


  RedisGeoSinkTask task;

  @BeforeEach
  public void before() {
    this.task = new RedisGeoSinkTask();
  }

  @Test
  public void emptyAssignment(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of());
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisCacheSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );
  }

  @Test
  public void putEmpty(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisCacheSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    this.task.put(ImmutableList.of());
  }


  @Test
  public void putWrite(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException, TimeoutException, IOException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisCacheSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    AtomicLong offset = new AtomicLong(1L);
    List<TestLocation> expected = TestLocation.loadLocations();
    List<SinkRecord> records = expected.stream()
        .map(l -> l.structWrite(topic, 1, offset))
        .collect(Collectors.toList());


    this.task.put(records);

    ListMultimap<SinkOperation.GeoSetKey, TestLocation> operations = locationByRegion(expected);

    for (SinkOperation.GeoSetKey key : operations.keySet()) {
      List<TestLocation> locations = operations.get(key);
      byte[][] keys = locations.stream()
          .map(l -> l.ident.getBytes(Charsets.UTF_8))
          .toArray(byte[][]::new);
      List<GeoCoordinates> result = this.task.session.asyncCommands().geopos(key.key, keys).get(30, TimeUnit.SECONDS);
      assertEquals(locations.size(), result.size());
      IntStream.range(0, keys.length).forEach(index -> {
        TestLocation expectedLocation = locations.get(index);
        GeoCoordinates actualLocation = result.get(index);
        assertEquals(expectedLocation.latitude, actualLocation.getX().doubleValue(), .0001D);
        assertEquals(expectedLocation.longitude, actualLocation.getY().doubleValue(), .0001D);
      });
    }
  }

  private ListMultimap<SinkOperation.GeoSetKey, TestLocation> locationByRegion(List<TestLocation> expected) {
    ListMultimap<SinkOperation.GeoSetKey, TestLocation> operations = LinkedListMultimap.create();
    expected.forEach(l -> {
          operations.put(
              SinkOperation.GeoSetKey.of(l.region.getBytes(Charsets.UTF_8), l.ident.getBytes(Charsets.UTF_8)),
              l
          );
        }
    );
    return operations;
  }

  @Test
  public void putDelete(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws
      ExecutionException, InterruptedException, IOException, TimeoutException {
    log.info("address = {}", address);
    final String topic = "putDelete";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisCacheSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    AtomicLong offset = new AtomicLong(1L);
    List<TestLocation> expected = TestLocation.loadLocations();
    List<SinkRecord> writes = expected.stream()
        .map(l -> l.structWrite(topic, 1, offset))
        .collect(Collectors.toList());
    List<SinkRecord> deletes = expected.stream()
        .map(l -> l.structDelete(topic, 1, offset))
        .collect(Collectors.toList());

    this.task.put(writes);
    ListMultimap<SinkOperation.GeoSetKey, TestLocation> operations = locationByRegion(expected);
    for (SinkOperation.GeoSetKey key : operations.keySet()) {
      List<TestLocation> locations = operations.get(key);
      byte[][] keys = locations.stream()
          .map(l -> l.ident.getBytes(Charsets.UTF_8))
          .toArray(byte[][]::new);
      List<GeoCoordinates> result = this.task.session.asyncCommands().geopos(key.key, keys).get(30, TimeUnit.SECONDS);
      assertEquals(locations.size(), result.size());
      IntStream.range(0, keys.length).forEach(index -> {
        TestLocation expectedLocation = locations.get(index);
        GeoCoordinates actualLocation = result.get(index);
        assertEquals(expectedLocation.latitude, actualLocation.getX().doubleValue(), .0001D);
        assertEquals(expectedLocation.longitude, actualLocation.getY().doubleValue(), .0001D);
      });
    }
    this.task.put(deletes);


    for (SinkOperation.GeoSetKey key : operations.keySet()) {
      List<TestLocation> locations = operations.get(key);
      byte[][] keys = locations.stream()
          .map(l -> l.ident.getBytes(Charsets.UTF_8))
          .toArray(byte[][]::new);
      List<GeoCoordinates> result = this.task.session.asyncCommands().geopos(key.key, keys).get(30, TimeUnit.SECONDS);
      assertEquals(locations.size(), result.size());
      IntStream.range(0, keys.length).forEach(index -> {
        TestLocation expectedLocation = locations.get(index);
        GeoCoordinates actualLocation = result.get(index);
        assertNull(actualLocation);
      });
    }
  }

  @AfterEach
  public void after() {
    if (null != this.task) {
      this.task.stop();
    }
  }

}
