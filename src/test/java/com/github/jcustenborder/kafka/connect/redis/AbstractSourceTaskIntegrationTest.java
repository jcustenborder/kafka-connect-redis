package com.github.jcustenborder.kafka.connect.redis;


import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractSourceTaskIntegrationTest<TASK extends SourceTask> extends AbstractTaskIntegrationTest<TASK> {
  protected SourceTaskContext sourceTaskContext;
  protected OffsetStorageReader offsetStorageReader;

  @Override
  protected void setupContext() {
    this.sourceTaskContext = mock(SourceTaskContext.class);
    this.offsetStorageReader = mock(OffsetStorageReader.class);
    when(this.sourceTaskContext.offsetStorageReader()).thenReturn(this.offsetStorageReader);
    this.task.initialize(this.sourceTaskContext);
  }
}
