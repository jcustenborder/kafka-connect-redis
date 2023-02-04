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

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

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

}
