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
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisClusterHealthCheck;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisSentinelHealthCheck;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisStandardHealthCheck;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.docker.compose.connection.Cluster;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Compose(
    dockerComposePath = "src/test/resources/docker-compose.yml"
)
public abstract class RedisGeoSinkTaskIT extends AbstractSinkTaskIntegrationTest<RedisGeoSinkTask> {
  private static final Logger log = LoggerFactory.getLogger(RedisGeoSinkTaskIT.class);

  @Override
  protected RedisGeoSinkTask createTask() {
    return new RedisGeoSinkTask();
  }


  @Test
  public void emptyAssignment() throws ExecutionException, InterruptedException {
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of());
    this.task.start(this.settings);
  }

  @Test
  public void putEmpty() throws ExecutionException, InterruptedException {
    this.task.start(this.settings);
    this.task.put(ImmutableList.of());
  }


  @Test
  public void putWrite() throws ExecutionException, InterruptedException, TimeoutException, IOException {
    when(sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(this.settings);

    AtomicLong offset = new AtomicLong(1L);
    List<TestLocation> expected = TestLocation.loadLocations();
    List<SinkRecord> records = expected.stream()
        .map(l -> l.structWrite(topic, 1, offset))
        .collect(Collectors.toList());

    this.task.put(records);

    Map<TopicPartition, OffsetAndMetadata> offsets = TestUtils.offsets(records);
    this.task.flush(offsets);

//    ListMultimap<SinkOperation.GeoSetKey, TestLocation> operations = locationByRegion(expected);
//
//    for (SinkOperation.GeoSetKey key : operations.keySet()) {
//      List<TestLocation> locations = operations.get(key);
//      byte[][] keys = locations.stream()
//          .map(l -> l.ident.getBytes(Charsets.UTF_8))
//          .toArray(byte[][]::new);
//      List<GeoCoordinates> result = this.task.session.asyncCommands().geopos(key.key, keys).get(30, TimeUnit.SECONDS);
//      assertEquals(locations.size(), result.size());
//      IntStream.range(0, keys.length).forEach(index -> {
//        TestLocation expectedLocation = locations.get(index);
//        GeoCoordinates actualLocation = result.get(index);
//        assertEquals(expectedLocation.latitude, actualLocation.getX().doubleValue(), .0001D);
//        assertEquals(expectedLocation.longitude, actualLocation.getY().doubleValue(), .0001D);
//      });
//    }
  }

//  private ListMultimap<SinkOperation.GeoSetKey, TestLocation> locationByRegion(List<TestLocation> expected) {
//    ListMultimap<SinkOperation.GeoSetKey, TestLocation> operations = LinkedListMultimap.create();
//    expected.forEach(l -> {
//          operations.put(
//              SinkOperation.GeoSetKey.of(l.region.getBytes(Charsets.UTF_8), l.ident.getBytes(Charsets.UTF_8)),
//              l
//          );
//        }
//    );
//    return operations;
//  }

  @Test
  public void putDelete() throws ExecutionException, InterruptedException, IOException, TimeoutException {
    when(sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(this.settings);

    AtomicLong offset = new AtomicLong(1L);
    List<TestLocation> expected = TestLocation.loadLocations();
    List<SinkRecord> writes = expected.stream()
        .map(l -> l.structWrite(topic, 1, offset))
        .collect(Collectors.toList());
    List<SinkRecord> deletes = expected.stream()
        .map(l -> l.structDelete(topic, 1, offset))
        .collect(Collectors.toList());

    this.task.put(writes);
    Map<TopicPartition, OffsetAndMetadata> offsets = TestUtils.offsets(writes);
    this.task.flush(offsets);
    
    //TODO: Add more verification to this class
//    ListMultimap<SinkOperation.GeoSetKey, TestLocation> operations = locationByRegion(expected);
//    for (SinkOperation.GeoSetKey key : operations.keySet()) {
//      List<TestLocation> locations = operations.get(key);
//      byte[][] keys = locations.stream()
//          .map(l -> l.ident.getBytes(Charsets.UTF_8))
//          .toArray(byte[][]::new);
//      List<GeoCoordinates> result = this.task.session.asyncCommands().geopos(key.key, keys).get(30, TimeUnit.SECONDS);
//      assertEquals(locations.size(), result.size());
//      IntStream.range(0, keys.length).forEach(index -> {
//        TestLocation expectedLocation = locations.get(index);
//        GeoCoordinates actualLocation = result.get(index);
//        assertEquals(expectedLocation.latitude, actualLocation.getX().doubleValue(), .0001D);
//        assertEquals(expectedLocation.longitude, actualLocation.getY().doubleValue(), .0001D);
//      });
//    }
    this.task.put(deletes);
    offsets = TestUtils.offsets(writes);
    this.task.flush(offsets);
//
//    for (SinkOperation.GeoSetKey key : operations.keySet()) {
//      List<TestLocation> locations = operations.get(key);
//      byte[][] keys = locations.stream()
//          .map(l -> l.ident.getBytes(Charsets.UTF_8))
//          .toArray(byte[][]::new);
//      List<GeoCoordinates> result = this.task.session.asyncCommands().geopos(key.key, keys).get(30, TimeUnit.SECONDS);
//      assertEquals(locations.size(), result.size());
//      IntStream.range(0, keys.length).forEach(index -> {
//        TestLocation expectedLocation = locations.get(index);
//        GeoCoordinates actualLocation = result.get(index);
//        assertNull(actualLocation);
//      });
//    }
  }

}
