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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordDeque;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordDequeBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class AbstractRedisSourceTask<CONFIG extends RedisSourceConnectorConfig> extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(AbstractRedisSourceTask.class);
  protected CONFIG config;
  protected SourceRecordDeque records;
  protected RedisSessionFactory sessionFactory = new RedisSessionFactoryImpl();


  protected abstract CONFIG config(Map<String, String> settings);

  @Override
  public void start(Map<String, String> settings) {
    this.config = config(settings);
    this.records = SourceRecordDequeBuilder.of()
        .emptyWaitMs(100)
        .batchSize(16384)
        .build();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    return this.records.getBatch();
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }


}
