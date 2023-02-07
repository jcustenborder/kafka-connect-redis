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

import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public interface RedisSession extends AutoCloseable {
  void flushCommands();


  void setAutoFlushCommands(boolean enabled);

//  RedisAsyncCommands<byte[], byte[]> asyncCommands();

  RedisGeoAsyncCommands<byte[], byte[]> geo();

  RedisHashAsyncCommands<byte[], byte[]> hash();

  RedisStringAsyncCommands<byte[], byte[]> string();

  RedisKeyAsyncCommands<byte[], byte[]> key();

  RedisSortedSetAsyncCommands<byte[], byte[]> sortedSet();

  RedisStreamAsyncCommands<byte[], byte[]> streams();
}
