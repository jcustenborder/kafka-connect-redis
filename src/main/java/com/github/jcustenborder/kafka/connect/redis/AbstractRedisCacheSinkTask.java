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
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import io.lettuce.core.KeyValue;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractRedisCacheSinkTask<CONFIG extends RedisSinkConnectorConfig> extends AbstractRedisSinkTask<CONFIG> {
  private static final Logger log = LoggerFactory.getLogger(AbstractRedisCacheSinkTask.class);

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

  static String redisOffsetKey(TopicPartition topicPartition) {
    return String.format("__kafka.offset.%s.%s", topicPartition.topic(), topicPartition.partition());
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    super.start(settings);

    log.debug("start() - Setting connection().setAutoFlushCommands(false)");
    this.session.setAutoFlushCommands(false);
  }


//  protected abstract void operations(SinkOperations sinkOperations, Collection<SinkRecord> records);


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
