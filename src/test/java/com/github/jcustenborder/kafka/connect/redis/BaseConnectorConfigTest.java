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
