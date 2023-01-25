package com.github.jcustenborder.kafka.connect.redis;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedisPubSubSourceTaskTest {
  RedisPubSubSourceTask task;
  Map<String, String> settings;
  RedisPubSubSession<byte[], byte[]> session;
  StatefulRedisPubSubConnection<byte[], byte[]> connection;
  RedisPubSubAsyncCommands<byte[], byte[]> commands;

  @BeforeEach
  public void before() {
    this.task = new RedisPubSubSourceTask();
    this.task.sessionFactory = mock(RedisSessionFactory.class);
    this.settings = new LinkedHashMap<>();

    this.session = mock(RedisPubSubSession.class);
    this.connection = mock(StatefulRedisPubSubConnection.class);
    this.commands = mock(RedisPubSubAsyncCommands.class);
    when(this.task.sessionFactory.createPubSubSession(any())).thenReturn(this.session);
    when(this.session.connection()).thenReturn(this.connection);
    when(this.session.asyncCommands()).thenReturn(this.commands);
  }

  void sendMessages(byte[] channel, List<String> messages) {
    messages.forEach(message -> {
          byte[] buffer = message.getBytes(StandardCharsets.UTF_8);
          this.task.message(channel, buffer);
        }
    );
  }


  void sendMessages(RedisClusterNode node, byte[] channel, List<String> messages) {
    messages.forEach(message -> {
          byte[] buffer = message.getBytes(StandardCharsets.UTF_8);
          this.task.message(node, channel, buffer);
        }
    );
  }

  void sendMessages(byte[] channel, byte[] pattern, List<String> messages) {
    messages.forEach(message -> {
          byte[] buffer = message.getBytes(StandardCharsets.UTF_8);
          this.task.message(pattern, channel, buffer);
        }
    );
  }

  void sendMessages(RedisClusterNode node, byte[] channel, byte[] pattern, List<String> messages) {
    messages.forEach(message -> {
          byte[] buffer = message.getBytes(StandardCharsets.UTF_8);
          this.task.message(node, pattern, channel, buffer);
        }
    );
  }

  @Test
  public void poll() throws InterruptedException {
    final String channelName = "channel0";
    final byte[] channel = channelName.getBytes(StandardCharsets.UTF_8);
    final String patternName = "channel*";
    final byte[] pattern = patternName.getBytes(StandardCharsets.UTF_8);
    this.settings.put(RedisPubSubSourceConnectorConfig.TOPIC_PREFIX_CONF, "redis.");
    this.settings.put(RedisPubSubSourceConnectorConfig.REDIS_CHANNELS_CONF, channelName);
    this.settings.put(RedisPubSubSourceConnectorConfig.REDIS_CHANNEL_PATTERNS_CONF, channelName);
    this.task.start(this.settings);
    final List<String> input = IntStream.range(0, 100)
        .boxed()
        .map(i -> String.format("message%s", i))
        .collect(Collectors.toList());

    sendMessages(channel, input);
    List<SourceRecord> records = this.task.poll();
    TestUtils.assertRecords(input, records, (message, record) -> {
      TestUtils.assertHeader(record, "redis.channel", channelName);
      assertNotNull(record.value(), "record value should not be null");
      byte[] actualBytes = (byte[]) record.value();
      String actual = new String(actualBytes, StandardCharsets.UTF_8);
      assertEquals(message, actual, "record.value() does not match");
    });

    sendMessages(channel, pattern, input);
    records = this.task.poll();
    TestUtils.assertRecords(input, records, (message, record) -> {
      TestUtils.assertHeader(record, "redis.channel", channelName);
      TestUtils.assertHeader(record, "redis.channel.pattern", patternName);
      assertNotNull(record.value(), "record value should not be null");
      byte[] actualBytes = (byte[]) record.value();
      String actual = new String(actualBytes, StandardCharsets.UTF_8);
      assertEquals(message, actual, "record.value() does not match");
    });

    RedisClusterNode node = new RedisClusterNode();
    node.setNodeId("1234");
    node.setUri(RedisURI.builder().withHost("redis").withPassword("testing").withPort(1234).build());
    final String nodeUri = node.getUri().toString();

    sendMessages(node, channel, input);
    records = this.task.poll();
    TestUtils.assertRecords(input, records, (message, record) -> {
      TestUtils.assertHeader(record, "redis.node.id", node.getNodeId());
      TestUtils.assertHeader(record, "redis.node.uri", nodeUri);
      TestUtils.assertHeader(record, "redis.channel", channelName);
      assertNotNull(record.value(), "record value should not be null");
      byte[] actualBytes = (byte[]) record.value();
      String actual = new String(actualBytes, StandardCharsets.UTF_8);
      assertEquals(message, actual, "record.value() does not match");
    });

    sendMessages(node, channel, pattern, input);
    records = this.task.poll();
    TestUtils.assertRecords(input, records, (message, record) -> {
      TestUtils.assertHeader(record, "redis.node.id", node.getNodeId());
      TestUtils.assertHeader(record, "redis.node.uri", nodeUri);
      TestUtils.assertHeader(record, "redis.channel", channelName);
      TestUtils.assertHeader(record, "redis.channel.pattern", patternName);
      assertNotNull(record.value(), "record value should not be null");
      byte[] actualBytes = (byte[]) record.value();
      String actual = new String(actualBytes, StandardCharsets.UTF_8);
      assertEquals(message, actual, "record.value() does not match");
    });

  }
}
