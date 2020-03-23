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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractRedisCacheSourceTask<CONFIG extends RedisSourceConnectorConfig> extends AbstractRedisSourceTask<CONFIG> {
  private static final Logger log = LoggerFactory.getLogger(AbstractRedisCacheSourceTask.class);
  protected RedisClusterSession<byte[], byte[]> session;

  @Override
  public void start(Map<String, String> settings) {
    this.config = config(settings);
    setup(this.config);
    this.session = this.sessionFactory.createClusterSession(this.config);
  }

  @Override
  public void stop() {
    try {
      log.debug("stop() - Closing session.");
      this.session.close();
    } catch (Exception e) {
      log.error("Exception thrown while closing session.", e);
    }
  }
}
