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
import io.lettuce.core.KeyValue;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class TestUtils {
  public static KeyValue<String, String> toString(KeyValue<byte[], byte[]> input) {
    return KeyValue.fromNullable(
        new String(input.getKey(), Charsets.UTF_8),
        new String(input.getValue(), Charsets.UTF_8)
    );
  }

  public static <T> void assertRecords(List<T> input, List<SourceRecord> records, BiConsumer<T, SourceRecord> consumer) {
    assertNotNull(records, "records should not be null");
    assertEquals(input.size(), records.size(), "records.size() does not match");
    for (int i = 0; i < input.size(); i++) {
      final T expected = input.get(i);
      final SourceRecord record = records.get(i);
      consumer.accept(expected, record);
    }
  }

  public static void assertHeader(SourceRecord record, String name, String expected) {
    Header header = record.headers().lastWithName(name);
    assertNotNull(header, String.format("Header '%s' was not found", name));
    switch (header.schema().type()) {
      case STRING:
        assertEquals(expected, header.value(), String.format("Header '%s' does not match.", name));
        break;
      case BYTES:
        assertEquals(expected, new String((byte[]) header.value(), StandardCharsets.UTF_8), String.format("Header '%s' does not match.", name));
        break;
      default:
        fail(String.format("Header '%s' was unexpected schema type of {}.", name, header.schema().type()));
        break;
    }
  }

  public static Map<String, String> mapOf(String... args) {
    assertEquals(0, args.length % 2, "Even number of arguments must be supplied");
    Map<String, String> result = new LinkedHashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      result.put(
          args[i],
          args[i + 1]
      );
    }
    return result;
  }

  public static Map<TopicPartition, OffsetAndMetadata> offsets(List<SinkRecord> records) {
    Map<TopicPartition, OffsetAndMetadata> result = new LinkedHashMap<>();
    for (SinkRecord record : records) {
      result.put(new TopicPartition(record.topic(), record.kafkaPartition()), new OffsetAndMetadata(record.kafkaOffset()));
    }
    return result;
  }
}
