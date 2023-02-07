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

import com.github.jcustenborder.docker.junit5.DockerCluster;
import com.palantir.docker.compose.connection.Cluster;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AbstractTaskIntegrationTest<TASK extends Task> {
  private static final Logger log = LoggerFactory.getLogger(AbstractTaskIntegrationTest.class);
  abstract protected void setupContext();
  abstract protected TASK createTask();

  abstract protected ConnectionHelper createConnectionHelper(Cluster cluster);

  protected TASK task;
  protected Map<String, String> settings;
  protected ConnectionHelper connectionHelper;

  protected TestInfo testInfo;
  @BeforeEach
  public void setupRedis(TestInfo testInfo, @DockerCluster Cluster cluster) {
    this.testInfo = testInfo;
    this.task = createTask();
    this.settings = new LinkedHashMap<>();
    this.connectionHelper = createConnectionHelper(cluster);
    this.connectionHelper.appendSettings(this.settings);
    setupContext();
  }

  @AfterEach
  public void after() {
    if (null != this.task) {
      log.info("Calling task.stop()");
      this.task.stop();
    }
  }
}
