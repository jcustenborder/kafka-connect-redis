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

import com.google.common.base.MoreObjects;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TestLocation {
  static final Schema KEY_SCHEMA = SchemaBuilder.struct()
      .field("key", Schema.STRING_SCHEMA)
      .field("member", Schema.STRING_SCHEMA)
      .build();
  static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
      .field("latitude", Schema.FLOAT64_SCHEMA)
      .field("longitude", Schema.FLOAT64_SCHEMA)
      .build();
  @CsvBindByPosition(position = 0)
  String ident;
  @CsvBindByPosition(position = 1)
  String region;
  @CsvBindByPosition(position = 2)
  Double latitude;
  @CsvBindByPosition(position = 3)
  Double longitude;

  public static List<TestLocation> loadLocations() throws IOException {
    try (InputStream iostr = TestLocation.class.getResourceAsStream("/airports.csv")) {
      try (InputStreamReader inputStreamReader = new InputStreamReader(iostr)) {
        ColumnPositionMappingStrategy strategy = new ColumnPositionMappingStrategy();
        strategy.setType(TestLocation.class);
        CsvToBean cv = new CsvToBeanBuilder(inputStreamReader)
            .withSeparator(',')
            .withMappingStrategy(strategy)
            .withSkipLines(1)
            .withType(TestLocation.class)
            .build();
        return cv.parse();
      }
    }


  }

  public String ident() {
    return this.ident;
  }

  public String region() {
    return this.region;
  }

  public Double latitude() {
    return this.latitude;
  }

  public Double longitude() {
    return this.longitude;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ident", ident)
        .add("region", region)
        .add("latitude", latitude)
        .add("longitude", longitude)
        .toString();
  }

  public SinkRecord structWrite(
      String topic,
      int partition,
      AtomicLong offset
  ) {
    Struct key = new Struct(KEY_SCHEMA)
        .put("key", this.region)
        .put("member", this.ident);
    Struct value = new Struct(VALUE_SCHEMA)
        .put("latitude", this.latitude)
        .put("longitude", this.longitude);

    return new SinkRecord(topic, partition, key.schema(), key, value.schema(), value, offset.incrementAndGet());
  }

  public SinkRecord structDelete(
      String topic,
      int partition,
      AtomicLong offset
  ) {
    Struct key = new Struct(KEY_SCHEMA)
        .put("key", this.region)
        .put("member", this.ident);

    return new SinkRecord(topic, partition, key.schema(), key, null, null, offset.incrementAndGet());
  }
}
