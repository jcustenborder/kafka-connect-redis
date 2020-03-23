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
import com.github.jcustenborder.kafka.connect.utils.config.TaskConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

public abstract class AbstractRedisSourceConnector<CONFIG extends AbstractConfig> extends SourceConnector {
  protected Map<String, String> settings;

  protected abstract CONFIG config(Map<String, String> settings);

  @Override
  public void start(Map<String, String> settings) {
    CONFIG config = config(settings);
    this.settings = settings;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int count) {
    return TaskConfigs.multiple(this.settings, count);
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
