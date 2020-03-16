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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RedisStreamsSourceTask extends SourceTask {
  static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
  static final Schema VALUE_SCHEMA = SchemaBuilder.map(
      Schema.STRING_SCHEMA,
      Schema.STRING_SCHEMA
  );
  private static final Logger log = LoggerFactory.getLogger(RedisStreamsSourceTask.class);
  protected RedisClusterSession<String, String> session;
  RedisStreamsSourceConnectorConfig config;
  RedisSessionFactory sessionFactory = new RedisSessionFactoryImpl();
  int streamCount;
  ListMultimap<String, String> messageIds;
  Map<String, XReadArgs.StreamOffset<String>> streamOffsets;
  XReadArgs xReadArgs;

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    this.config = new RedisStreamsSourceConnectorConfig(settings);
    log.info("start() - Determining source offset(s) for {}", this.config.streams);
    this.streamCount = this.config.streams.size();
    this.messageIds = LinkedListMultimap.create(this.streamCount);
    this.streamOffsets = new LinkedHashMap<>(this.streamCount);
    this.xReadArgs = XReadArgs.Builder.count(5000);

    RedisCodec<String, String> stringCodec = new StringCodec(this.config.charset);
    this.session = sessionFactory.createClusterSession(this.config, stringCodec);

    for (String stream : this.config.streams) {
      Map<String, String> sourcePartition = ImmutableMap.of("stream", stream);
      log.trace("start() - Looking up offset for '{}'", sourcePartition);
      Map<String, Object> sourceOffset = this.context.offsetStorageReader().offset(sourcePartition);
      log.trace("start() - Offset returned for '{}' is '{}'", sourcePartition, sourceOffset);
      String offset;
      if (null == sourceOffset || sourceOffset.isEmpty() || !sourceOffset.containsKey("offset")) {
        offset = "0";
      } else {
        offset = (String) sourceOffset.getOrDefault("offset", "0");
      }

      XReadArgs.StreamOffset<String> streamOffset = XReadArgs.StreamOffset.from(stream, offset);
      log.info("start() - Setting Redis Stream offset for '{}' to '{}'", stream, streamOffset);
      this.streamOffsets.put(streamOffset.getName(), streamOffset);
    }


  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    this.messageIds.clear();
    XReadArgs.StreamOffset<String>[] streamOffsets = this.streamOffsets.values().toArray(new XReadArgs.StreamOffset[0]);
    if (log.isTraceEnabled()) {
      log.trace(
          "poll() - Calling xreadgroup. streamOffsets = '{}'",
          Joiner.on(", ").join(streamOffsets)
      );
    }

    RedisFuture<List<StreamMessage<String, String>>> future = this.session.asyncCommands().xread(xReadArgs, streamOffsets);
    List<StreamMessage<String, String>> result;
    result = LettuceFutures.awaitOrCancel(future, this.config.operationTimeoutMs, TimeUnit.MILLISECONDS);
    final int messageCount = result.size();
    log.trace("poll() - {} message(s) returned from Redis.", messageCount);
    if (0 == messageCount) {
      return null;
    }
    List<SourceRecord> records = new ArrayList<>(messageCount);
    Map<String, Map<String, String>> sourcePartitions = new HashMap<>();
    for (StreamMessage<String, String> message : result) {
      Map<String, String> sourcePartition = sourcePartitions.computeIfAbsent(
          message.getStream(),
          s -> ImmutableMap.of("stream", message.getStream())
      );
      Map<String, String> sourceOffset = ImmutableMap.of("offset", message.getId());
      records.add(
          new SourceRecord(
              sourcePartition,
              sourceOffset,
              message.getStream(),
              KEY_SCHEMA,
              message.getId(),
              VALUE_SCHEMA,
              message.getBody()
          )
      );
      this.messageIds.put(message.getStream(), message.getId());
    }
    return records;
  }

  @Override
  public void commit() throws InterruptedException {
    Map<String, XReadArgs.StreamOffset<String>> streamOffsets = new LinkedHashMap<>(this.streamCount);
    List<RedisFuture<Long>> results = new ArrayList<>(this.streamCount);
    for (String stream : this.messageIds.keySet()) {
      List<String> messageIds = this.messageIds.get(stream);
      results.add(
          this.session.asyncCommands().xack(stream, "null", messageIds.toArray(new String[0]))
      );
      String lastMessage = messageIds.get(messageIds.size() - 1);
      streamOffsets.put(stream, XReadArgs.StreamOffset.from(stream, lastMessage));
    }
    if (!LettuceFutures.awaitAll(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS, results.toArray(new RedisFuture[0]))) {
      throw new IllegalStateException();
    }
    this.streamOffsets.putAll(streamOffsets);
  }

  @Override
  public void stop() {
    try {
      if (null != this.session) {
        this.session.close();
      }
    } catch (Exception e) {
      log.warn("Exception thrown", e);
    }
  }
}
