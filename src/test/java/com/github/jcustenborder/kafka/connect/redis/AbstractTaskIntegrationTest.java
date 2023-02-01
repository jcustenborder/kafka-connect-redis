package com.github.jcustenborder.kafka.connect.redis;

import com.github.jcustenborder.docker.junit5.DockerCluster;
import com.palantir.docker.compose.connection.Cluster;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AbstractTaskIntegrationTest<TASK extends Task> {

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


}
