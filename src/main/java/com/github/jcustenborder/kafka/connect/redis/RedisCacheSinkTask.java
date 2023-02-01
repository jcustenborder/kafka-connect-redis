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

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.github.jcustenborder.kafka.connect.redis.Utils.logLocation;

public class RedisCacheSinkTask extends AbstractRedisCacheSinkTask<RedisSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(RedisCacheSinkTask.class);

  @Override
  protected RedisSinkConnectorConfig config(Map<String, String> settings) {
    return new RedisSinkConnectorConfig(settings);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("put() - Processing {} record(s)", records.size());

    for (SinkRecord record : records) {
      logLocation(log, record);
      byte[] key = toBytes("key", record.key());
      byte[] value = toBytes("value", record.value());

      CompletableFuture<?> future;

      if (null != value) {
        future = this.session.asyncCommands().set(key, value)
            .exceptionally(exceptionally(record))
            .toCompletableFuture();
      } else {
        future = this.session.asyncCommands().del(key)
            .exceptionally(exceptionally(record))
            .toCompletableFuture();
      }
      this.futures.add(future);
    }
    log.debug("put() - Added {} future(s)", this.futures.size());
  }
}
