package com.github.jcustenborder.kafka.connect.redis;

import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.BeforeEach;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AbstractIntegrationTest<TASK extends Task> {

  abstract protected TASK createTask();

  protected TASK task;
  protected Map<String, String> settings;

  @BeforeEach
  public void before() {
    this.task = createTask();
    this.settings = new LinkedHashMap<>();
  }

}
