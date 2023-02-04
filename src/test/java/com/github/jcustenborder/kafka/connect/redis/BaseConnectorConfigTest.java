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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class BaseConnectorConfigTest<T extends RedisConnectorConfig> {
  protected Map<String, String> settings;

  @BeforeEach
  public void before() {
    this.settings = new LinkedHashMap<>();
  }

  void set(String... args) {
    assertEquals(0, args.length % 2, "An even number of arguments should be passed.");
    for (int i = 0; i < args.length; i += 2) {
      String key = args[i];
      String value = args[i + 1];
      this.settings.put(key, value);
    }
  }

  protected abstract T newConnectorConfig();

  @Test
  public void testSet() {
    set("foo", "bar", "test", "baz");
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("foo", "bar");
    expected.put("test", "baz");
    assertEquals(expected, this.settings);
  }

  @Test
  public void clientMode() {

  }


  @Test
  public void foo() {
    set(
        "foo", "bar"
    );

  }


}
