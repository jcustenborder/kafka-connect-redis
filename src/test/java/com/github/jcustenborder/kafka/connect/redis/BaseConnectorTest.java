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

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class BaseConnectorTest<CONNECTOR extends Connector, TASK extends Task> {
  protected abstract Class<TASK> expectedTaskClass();

  protected abstract CONNECTOR newConnector();

  protected CONNECTOR connector;

  @BeforeEach
  public void before() {
    this.connector = newConnector();
  }

  @Test
  public void configShouldNotBeNull() {
    assertNotNull(this.connector.config(), "Connector config() should not be null.");
  }

  @Test
  public void ensureConnectorTask() {
    assertEquals(expectedTaskClass(), this.connector.taskClass(), "Task class does not match.");
  }
}
