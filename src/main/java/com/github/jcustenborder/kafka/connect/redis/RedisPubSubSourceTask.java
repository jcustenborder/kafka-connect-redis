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

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.pubsub.RedisPubSubListener;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RedisPubSubSourceTask extends AbstractRedisPubSubSourceTask<RedisPubSubSourceConnectorConfig>
    implements RedisPubSubListener<byte[], byte[]>, RedisClusterPubSubListener<byte[], byte[]> {
  private static final Logger log = LoggerFactory.getLogger(RedisPubSubSourceTask.class);

  @Override
  public void start(Map<String, String> settings) {
    super.start(settings);
    this.pubSubSession.addPubSubListener(this);
    this.pubSubSession.addClusterPubSubListener(this);

    List<CompletableFuture<?>> futures = new ArrayList<>();

    byte[][] subscribeChannels = this.config.channels.stream()
        .map(s -> s.getBytes(this.config.charset))
        .toArray(byte[][]::new);
    if (subscribeChannels.length > 0) {
      log.info("start() - Subscribing to {}", this.config.channels);
      futures.add(
          this.pubSubSession.asyncCommands().subscribe(subscribeChannels)
              .exceptionally(e -> {
                log.error("start() - error while calling subscribe", e);
                return null;
              })
              .toCompletableFuture()
      );
    }
    byte[][] subscribePatterns = this.config.channelPatterns.stream()
        .map(s -> s.getBytes(this.config.charset))
        .toArray(byte[][]::new);
    if (subscribePatterns.length > 0) {
      log.info("start() - Subscribing to patterns {}", this.config.channelPatterns);
      futures.add(
          this.pubSubSession.asyncCommands().psubscribe(subscribePatterns)
              .exceptionally(e -> {
                log.error("start() - error while calling psubscribe()", e);
                return null;
              })
              .toCompletableFuture()
      );
    }
    LettuceFutures.awaitAll(30, TimeUnit.SECONDS, futures.toArray(new CompletableFuture<?>[0]));
  }

  @Override
  protected RedisPubSubSourceConnectorConfig config(Map<String, String> settings) {
    return new RedisPubSubSourceConnectorConfig(settings);
  }

  void addRecord(RedisClusterNode node, byte[] channel, byte[] pattern, byte[] message) {
    final String channelName = new String(channel, this.config.charset);
    String topic = this.config.topicPrefix + channelName;
    log.trace("addRecord() - channelName = '{}' topic = '{}'", channelName, topic);

    ConnectHeaders headers = new ConnectHeaders();
    headers.addBytes("redis.channel", channel);

    if (null != pattern) {
      headers.addBytes("redis.channel.pattern", pattern);
    }

    if (null != node) {
      headers.addString("redis.node.id", node.getNodeId());
      headers.addString("redis.node.uri", node.getUri().toString());
    }

    this.records.add(
        new SourceRecord(
            null,
            null,
            topic,
            null,
            null,
            null,
            Schema.BYTES_SCHEMA,
            message,
            null,
            headers
        )
    );
  }

  @Override
  public void message(byte[] channel, byte[] message) {
    addRecord(null, channel, null, message);
  }

  @Override
  public void message(byte[] pattern, byte[] channel, byte[] message) {
    addRecord(null, channel, pattern, message);
  }

  @Override
  public void message(RedisClusterNode node, byte[] channel, byte[] message) {
    addRecord(node, channel, null, message);
  }

  @Override
  public void message(RedisClusterNode node, byte[] pattern, byte[] channel, byte[] message) {
    addRecord(node, channel, pattern, message);
  }

  @Override
  public void subscribed(byte[] channel, long count) {
    final String channelName = new String(channel, StandardCharsets.UTF_8);
    log.info("subscribed(channel = '{}', count = {})", channelName, count);
  }

  @Override
  public void psubscribed(byte[] pattern, long count) {
    final String patternName = new String(pattern, StandardCharsets.UTF_8);
    log.info("psubscribed(pattern = '{}', count = {})", patternName, count);
  }

  @Override
  public void unsubscribed(byte[] channel, long count) {
    final String channelName = new String(channel, StandardCharsets.UTF_8);
    log.info("unsubscribed(channel = '{}', count = {})", channelName, count);
  }

  @Override
  public void punsubscribed(byte[] pattern, long count) {
    final String patternName = new String(pattern, StandardCharsets.UTF_8);
    log.info("punsubscribed(pattern = '{}', count = {})", patternName, count);
  }

  @Override
  public void subscribed(RedisClusterNode node, byte[] channel, long count) {
    final String channelName = new String(channel, StandardCharsets.UTF_8);
    log.info("subscribed(node = '{}', channel = '{}', count = {})", node.getNodeId(), channelName, count);
  }

  @Override
  public void psubscribed(RedisClusterNode node, byte[] channel, long count) {
    final String channelName = new String(channel, StandardCharsets.UTF_8);
    log.info("psubscribed(node = '{}', channel = '{}', count = {})", node.getNodeId(), channelName, count);
  }

  @Override
  public void unsubscribed(RedisClusterNode node, byte[] channel, long count) {
    final String channelName = new String(channel, StandardCharsets.UTF_8);
    log.info("unsubscribed(node = '{}', channel = '{}', count = {})", node.getNodeId(), channelName, count);
  }

  @Override
  public void punsubscribed(RedisClusterNode node, byte[] channel, long count) {
    final String channelName = new String(channel, StandardCharsets.UTF_8);
    log.info("punsubscribed(node = '{}', channel = '{}', count = {})", node.getNodeId(), channelName, count);
  }
}
