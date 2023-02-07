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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisCacheSinkConnectorConfigTest extends BaseConnectorConfigTest<RedisCacheSinkConnectorConfig> {
  @Override
  protected RedisCacheSinkConnectorConfig newConnectorConfig() {
    return new RedisCacheSinkConnectorConfig(this.settings);
  }

  @Test
  public void ttlMilliseconds() {
    set(RedisCacheSinkConnectorConfig.TTL_MS_CONFIG, "1");
    assertEquals(1L, newConnectorConfig().ttl);
  }

  @Test
  public void ttlSeconds() {
    set(RedisCacheSinkConnectorConfig.TTL_SECONDS_CONFIG, "1");
    assertEquals(1000L, newConnectorConfig().ttl);
  }

  @Test
  public void ttlMinutes() {
    set(RedisCacheSinkConnectorConfig.TTL_MINUTES_CONFIG, "1");
    assertEquals(60000, newConnectorConfig().ttl);
  }

  @Test
  public void ttlHours() {
    set(RedisCacheSinkConnectorConfig.TTL_HOURS_CONFIG, "1");
    assertEquals(3600000, newConnectorConfig().ttl);
  }

  @Test
  public void ttlDays() {
    set(RedisCacheSinkConnectorConfig.TTL_DAYS_CONFIG, "1");
    assertEquals(86400000, newConnectorConfig().ttl);
  }

  @Test
  public void ttlCombination() {
    set(
        RedisCacheSinkConnectorConfig.TTL_MS_CONFIG, "1",
        RedisCacheSinkConnectorConfig.TTL_SECONDS_CONFIG, "1",
        RedisCacheSinkConnectorConfig.TTL_MINUTES_CONFIG, "1",
        RedisCacheSinkConnectorConfig.TTL_HOURS_CONFIG, "1",
        RedisCacheSinkConnectorConfig.TTL_DAYS_CONFIG, "1"
    );
    assertEquals(90061001, newConnectorConfig().ttl);
  }
}
