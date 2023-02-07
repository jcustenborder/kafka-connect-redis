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

import com.google.common.collect.ImmutableMap;
import io.lettuce.core.RedisFuture;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.github.jcustenborder.kafka.connect.redis.TestUtils.offsets;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@Disabled
public class RedisPubSubSinkTaskTest extends AbstractSinkTaskTest<RedisPubSubSinkTask> {
  @Override
  protected RedisPubSubSinkTask createTask() {
    return null;
  }

//  @Test
//  public void test() {
//    this.task.start(ImmutableMap.of(
//        RedisPubSubSinkConnectorConfig.TARGET_CONFIG, "one,two,three"
//    ));
//    when(this.redisPubSubAsyncCommands.publish(any(), any())).thenAnswer(invocationOnMock -> {
//      RedisFuture<?> future = mock(RedisFuture.class, withSettings().verboseLogging());
//      when(future.exceptionally(any())).thenAnswer((Answer<Object>) invocationOnMock12 -> {
//        CompletionStage<?> stage = mock(CompletionStage.class);
//        when(stage.toCompletableFuture()).thenAnswer(invocationOnMock1 -> new CompletableFuture<>());
//        return stage;
//      });
//      return mock(RedisFuture.class, withSettings().verboseLogging());
//    });
//
//    List<SinkRecord> records = Arrays.asList(
//        record("set1", "one"),
//        record("set2", "two"),
//        record("delete1", null),
//        record("set3", "three"),
//        record("set4", "four")
//    );
//    this.task.put(records);
//    Map<TopicPartition, OffsetAndMetadata> offsets = offsets(records);
//    this.task.flush(offsets);
//  }
//
//  @Override
//  protected RedisPubSubSinkTask createTask() {
//    return new RedisPubSubSinkTask();
//  }
//

}
