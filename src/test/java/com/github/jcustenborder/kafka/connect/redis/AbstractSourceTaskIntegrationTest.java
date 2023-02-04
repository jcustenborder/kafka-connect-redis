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


import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractSourceTaskIntegrationTest<TASK extends SourceTask> extends AbstractTaskIntegrationTest<TASK> {
  protected SourceTaskContext sourceTaskContext;
  protected OffsetStorageReader offsetStorageReader;

  protected String topic;

  @Override
  protected void setupContext() {
    this.sourceTaskContext = mock(SourceTaskContext.class);
    this.offsetStorageReader = mock(OffsetStorageReader.class);
    when(this.sourceTaskContext.offsetStorageReader()).thenReturn(this.offsetStorageReader);
    this.task.initialize(this.sourceTaskContext);
    this.topic = this.testInfo.getTestMethod().get().getName();
  }
}
