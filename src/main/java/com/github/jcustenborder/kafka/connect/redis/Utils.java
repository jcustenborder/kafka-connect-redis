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

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;


class Utils {
  public static void logLocation(Logger log, SinkRecord record) {
    if (log.isTraceEnabled()) {
      log.trace("put() - Processing record " + formatLocation(record));
    }
  }

  static String formatLocation(SinkRecord record) {
    return String.format(
        "topic = %s partition = %s offset = %s",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset()
    );
  }
}
