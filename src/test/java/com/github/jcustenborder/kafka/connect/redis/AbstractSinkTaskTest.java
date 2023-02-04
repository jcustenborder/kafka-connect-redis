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
import com.google.common.base.Strings;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public abstract class AbstractSinkTaskTest<TASK extends AbstractRedisSinkTask> {
  protected SinkTaskContext context;
  protected ErrantRecordReporter errantRecordReporter;

  protected TASK task;

  protected abstract TASK createTask();

  protected RedisClusterSession<byte[], byte[]> redisClusterSession;
  protected RedisPubSubSession<byte[], byte[]> redisPubSubSession;

  protected StatefulRedisConnection<byte[], byte[]> redisConnection;
  protected RedisClusterAsyncCommands<byte[], byte[]> redisCommands;

  protected RedisPubSubAsyncCommands<byte[], byte[]> redisPubSubAsyncCommands;
  @BeforeEach
  public void before() {
    this.context = mock(SinkTaskContext.class);
    this.errantRecordReporter = mock(ErrantRecordReporter.class);
    when(this.context.errantRecordReporter()).thenReturn(this.errantRecordReporter);
    this.task = createTask();
    this.task.initialize(this.context);
    this.task.sessionFactory = mock(RedisSessionFactory.class);

    this.redisClusterSession = mock(RedisClusterSession.class);
    this.redisConnection = mock(StatefulRedisConnection.class);
    when(this.redisClusterSession.connection()).thenReturn(this.redisConnection);
    this.redisCommands = mock(RedisClusterAsyncCommands.class);
    when(this.redisClusterSession.asyncCommands()).thenReturn(this.redisCommands);

    this.redisPubSubSession = mock(RedisPubSubSession.class);
    when(this.task.sessionFactory.createPubSubSession(any())).thenReturn(this.redisPubSubSession);
    when(this.task.sessionFactory.createClusterSession(any())).thenReturn(this.redisClusterSession);

    this.redisPubSubAsyncCommands = mock(RedisPubSubAsyncCommands.class);
    when(this.redisPubSubSession.asyncCommands()).thenReturn(this.redisPubSubAsyncCommands);


  }

  long offset;
  protected SinkRecord record(String k, String v) {
    final byte[] key = k.getBytes(Charsets.UTF_8);
    final Schema keySchema = Schema.BYTES_SCHEMA;
    final byte[] value;
    final Schema valueSchema;

    if (Strings.isNullOrEmpty(v)) {
      value = null;
      valueSchema = null;
    } else {
      value = v.getBytes(Charsets.UTF_8);
      valueSchema = Schema.BYTES_SCHEMA;
    }

    return new SinkRecord(
        "topic",
        1,
        keySchema,
        key,
        valueSchema,
        value,
        offset++
    );

  }

}
