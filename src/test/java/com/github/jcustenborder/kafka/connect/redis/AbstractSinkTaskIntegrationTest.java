package com.github.jcustenborder.kafka.connect.redis;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractSinkTaskIntegrationTest<Task extends SinkTask> extends AbstractTaskIntegrationTest<Task> {
  protected SinkTaskContext sinkTaskContext;
  protected ErrantRecordReporter errantRecordReporter;

  protected String topic;

  @Override
  protected void setupContext() {
    this.sinkTaskContext = mock(SinkTaskContext.class);
    this.errantRecordReporter = mock(ErrantRecordReporter.class);
    when(this.sinkTaskContext.errantRecordReporter()).thenReturn(this.errantRecordReporter);
    this.task.initialize(this.sinkTaskContext);
    this.topic = this.testInfo.getTestMethod().get().getName();
  }

  protected Map<TopicPartition, OffsetAndMetadata> offsets(List<SinkRecord> records) {
    Map<TopicPartition, OffsetAndMetadata> result = new LinkedHashMap<>();
    for (SinkRecord record : records) {
      result.put(new TopicPartition(record.topic(), record.kafkaPartition()), new OffsetAndMetadata(record.kafkaOffset()));
    }
    return result;
  }
}
