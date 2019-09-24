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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.data.TopicPartitionCounter;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Charsets;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class RedisSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);
  private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSSZ");

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  RedisSinkConnectorConfig config;
  RedisSession session;

  static SinkOffsetState state(KeyValue<byte[], byte[]> input) {
    if (!input.hasValue()) {
      return null;
    }
    try {
      return ObjectMapperFactory.INSTANCE.readValue(input.getValue(), SinkOffsetState.class);
    } catch (IOException e) {
      throw new DataException(e);
    }
  }


  @Override
  public void start(Map<String, String> settings) {
    this.config = new RedisSinkConnectorConfig(settings);
    this.session = RedisSessionImpl.create(this.config);

    final Set<TopicPartition> assignment = this.context.assignment();
    if (!assignment.isEmpty()) {
      final byte[][] partitionKeys = assignment.stream()
          .map(RedisSinkTask::redisOffsetKey)
          .map(s -> s.getBytes(Charsets.UTF_8))
          .toArray(byte[][]::new);

      final RedisFuture<List<KeyValue<byte[], byte[]>>> partitionKeyFuture = this.session.asyncCommands().mget(partitionKeys);
      final List<SinkOffsetState> sinkOffsetStates;
      try {
        final List<KeyValue<byte[], byte[]>> partitionKey = partitionKeyFuture.get(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS);
        sinkOffsetStates = partitionKey.stream()
            .map(RedisSinkTask::state)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RetriableException(e);
      }
      Map<TopicPartition, Long> partitionOffsets = new HashMap<>(assignment.size());
      for (SinkOffsetState state : sinkOffsetStates) {
        partitionOffsets.put(state.topicPartition(), state.offset());
        log.info("Requesting offset {} for {}", state.offset(), state.topicPartition());
      }
      for (TopicPartition topicPartition : assignment) {
        if (!partitionOffsets.containsKey(topicPartition)) {
          partitionOffsets.put(topicPartition, 0L);
          log.info("Requesting offset {} for {}", 0L, topicPartition);
        }
      }
      this.context.offset(partitionOffsets);
    }
  }

  private byte[] toBytes(String source, Object input) {
    final byte[] result;

    if (input instanceof String) {
      String s = (String) input;
      result = s.getBytes(this.config.charset);
    } else if (input instanceof byte[]) {
      result = (byte[]) input;
    } else if (null == input) {
      result = null;
    } else if (input instanceof Struct) {
      Struct struct = (Struct) input;
      ObjectNode node = JsonNodeFactory.instance.objectNode();
      for (Field field : struct.schema().fields()) {
        node.set(field.name(), convertToJson(field.schema(), struct.get(field)));
      }
      result = node.toString().getBytes(this.config.charset);
    } else {
      throw new DataException(
          String.format(
              "The %s for the record must be String or Bytes. Consider using the ByteArrayConverter " +
                  "or StringConverter if the data is stored in Kafka in the format needed in Redis. " +
                  "Another option is to use a single message transformation to transform the data before " +
                  "it is written to Redis.",
              source
          )
      );
    }

    return result;
  }
  private static JsonNode convertToJson(Schema schema, Object logicalValue) {
    if (logicalValue == null) {
      if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
        return null;
      if (schema.defaultValue() != null)
        return convertToJson(schema, schema.defaultValue());
      if (schema.isOptional())
        return JsonNodeFactory.instance.nullNode();
      throw new DataException("Conversion error: null value for field that is required and has no default value");
    }

    try {
      final Schema.Type schemaType;
      if (schema == null) {
        schemaType = ConnectSchema.schemaType(logicalValue.getClass());
        if (schemaType == null)
          throw new DataException("Java class " + logicalValue.getClass() + " does not have corresponding schema type.");
      } else {
        schemaType = schema.type();
      }
      switch (schemaType) {
        case INT8:
          return JsonNodeFactory.instance.numberNode((Byte) logicalValue);
        case INT16:
          return JsonNodeFactory.instance.numberNode((Short) logicalValue);
        case INT32:
          if (schema != null && Date.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.textNode(ISO_DATE_FORMAT.format((java.util.Date) logicalValue));
          }
          if (schema != null && Time.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.textNode(TIME_FORMAT.format((java.util.Date) logicalValue));
          }
          return JsonNodeFactory.instance.numberNode((Integer) logicalValue);
        case INT64:
          String schemaName = schema.name();
          if (Timestamp.LOGICAL_NAME.equals(schemaName)) {
            return JsonNodeFactory.instance.numberNode(Timestamp.fromLogical(schema, (java.util.Date) logicalValue));
          }
          return JsonNodeFactory.instance.numberNode((Long) logicalValue);
        case FLOAT32:
          return JsonNodeFactory.instance.numberNode((Float) logicalValue);
        case FLOAT64:
          return JsonNodeFactory.instance.numberNode((Double) logicalValue);
        case BOOLEAN:
          return JsonNodeFactory.instance.booleanNode((Boolean) logicalValue);
        case STRING:
          CharSequence charSeq = (CharSequence) logicalValue;
          return JsonNodeFactory.instance.textNode(charSeq.toString());
        case BYTES:
          if (Decimal.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.numberNode((BigDecimal) logicalValue);
          }

          byte[] valueArr = null;
          if (logicalValue instanceof byte[])
            valueArr = (byte[]) logicalValue;
          else if (logicalValue instanceof ByteBuffer)
            valueArr = ((ByteBuffer) logicalValue).array();

          if (valueArr == null)
            throw new DataException("Invalid type for bytes type: " + logicalValue.getClass());

          return JsonNodeFactory.instance.binaryNode(valueArr);

        case ARRAY: {
          Collection collection = (Collection) logicalValue;
          ArrayNode list = JsonNodeFactory.instance.arrayNode();
          for (Object elem : collection) {
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode fieldValue = convertToJson(valueSchema, elem);
            list.add(fieldValue);
          }
          return list;
        }
        case MAP: {
          Map<?, ?> map = (Map<?, ?>) logicalValue;
          // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
          boolean objectMode;
          if (schema == null) {
            objectMode = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
              if (!(entry.getKey() instanceof String)) {
                objectMode = false;
                break;
              }
            }
          } else {
            objectMode = schema.keySchema().type() == Schema.Type.STRING;
          }
          ObjectNode obj = null;
          ArrayNode list = null;
          if (objectMode)
            obj = JsonNodeFactory.instance.objectNode();
          else
            list = JsonNodeFactory.instance.arrayNode();
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            Schema keySchema = schema == null ? null : schema.keySchema();
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode mapKey = convertToJson(keySchema, entry.getKey());
            JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

            if (objectMode)
              obj.set(mapKey.asText(), mapValue);
            else
              list.add(JsonNodeFactory.instance.arrayNode().add(mapKey).add(mapValue));
          }
          return objectMode ? obj : list;
        }
        case STRUCT: {
          Struct struct = (Struct) logicalValue;
          if (struct.schema() != schema)
            throw new DataException("Mismatching schema.");
          ObjectNode obj = JsonNodeFactory.instance.objectNode();
          for (Field field : schema.fields()) {
            obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
          }
          return obj;
        }
      }

      throw new DataException("Couldn't convert " + logicalValue + " to JSON.");
    } catch (ClassCastException e) {
      throw new DataException("Invalid type for " + schema.type() + ": " + logicalValue.getClass());
    }
  }


  static String formatLocation(SinkRecord record) {
    return String.format(
        "topic = %s partition = %s offset = %s",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset()
    );
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("put() - Processing {} record(s)", records.size());
    List<SinkOperation> operations = new ArrayList<>(records.size());

    SinkOperation operation = SinkOperation.NONE;

    TopicPartitionCounter counter = new TopicPartitionCounter();

    for (SinkRecord record : records) {
      log.trace("put() - Processing record " + formatLocation(record));
      if (null == record.key()) {
        throw new DataException(
            "The key for the record cannot be null. " + formatLocation(record)
        );
      }
      final byte[] key = toBytes("key", record.key());
      if (null == key || key.length == 0) {
        throw new DataException(
            "The key cannot be an empty byte array. " + formatLocation(record)
        );
      }

      final byte[] value = toBytes("value", record.value());

      SinkOperation.Type currentOperationType;

      if (null == value) {
        currentOperationType = SinkOperation.Type.DELETE;
      } else {
        currentOperationType = config.type;
      }

      if (currentOperationType != operation.type) {
        log.debug(
            "put() - Creating new operation. current={} last={}",
            currentOperationType,
            operation.type
        );
        operation = SinkOperation.create(currentOperationType, this.config, records.size());
        operations.add(operation);
      }
      operation.add(key, value);
      counter.increment(record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }

    log.debug(
        "put() - Found {} operation(s) in {} record{s}. Executing operations...",
        operations.size(),
        records.size()
    );

    final List<SinkOffsetState> offsetData = counter.offsetStates();
    if (!offsetData.isEmpty()) {
      operation = SinkOperation.create(SinkOperation.Type.SET, this.config, offsetData.size());
      operations.add(operation);
      for (SinkOffsetState e : offsetData) {
        final byte[] key = String.format("__kafka.offset.%s.%s", e.topic(), e.partition()).getBytes(Charsets.UTF_8);
        final byte[] value;
        try {
          value = ObjectMapperFactory.INSTANCE.writeValueAsBytes(e);
        } catch (JsonProcessingException e1) {
          throw new DataException(e1);
        }
        operation.add(key, value);
        log.trace("put() - Setting offset: {}", e);
      }
    }

    for (SinkOperation op : operations) {
      log.debug("put() - Executing {} operation with {} values", op.type, op.size());
      try {
        op.execute(this.session.asyncCommands());
      } catch (InterruptedException e) {
        throw new RetriableException(e);
      }
    }
  }

  private static String redisOffsetKey(TopicPartition topicPartition) {
    return String.format("__kafka.offset.%s.%s", topicPartition.topic(), topicPartition.partition());
  }

  @Override
  public void stop() {
    try {
      this.session.close();
    } catch (Exception e) {
      log.warn("Exception thrown", e);
    }
  }
}
