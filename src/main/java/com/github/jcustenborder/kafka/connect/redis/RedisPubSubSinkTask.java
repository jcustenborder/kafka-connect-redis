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

import static com.github.jcustenborder.kafka.connect.redis.Utils.logLocation;

public class RedisPubSubSinkTask extends AbstractRedisSinkTask<RedisPubSubSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(RedisPubSubSinkTask.class);
  protected RedisPubSubSession pubSubSession;

  @Override
  protected RedisPubSubSinkConnectorConfig config(Map<String, String> settings) {
    return new RedisPubSubSinkConnectorConfig(settings);
  }

  @Override
  public String version() {
    return null;
  }

  @Override
  public void start(Map<String, String> settings) {
    super.start(settings);
    this.pubSubSession = this.sessionFactory.createPubSubSession(this.config);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      logLocation(log, record);

      if (null == record.value()) {
        continue;
      }

      final byte[] value = toBytes("value", record.value());
      this.config.target.stream()
          .map(target ->
              this.pubSubSession.asyncCommands().publish(target, value)
                  .exceptionally(exceptionally(record))
                  .toCompletableFuture()
          ).forEach(this.futures::add);
      log.debug("put() - Added {} future(s)", this.futures.size());
    }
  }

}
