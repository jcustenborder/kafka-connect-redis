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

import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

abstract class SinkOperation {
  private static final Logger log = LoggerFactory.getLogger(SinkOperation.class);
  public static final SinkOperation NONE = new NoneOperation(null);

  public final Type type;
  private final RedisSinkConnectorConfig config;

  SinkOperation(Type type, RedisSinkConnectorConfig config) {
    this.type = type;
    this.config = config;
  }

  public enum Type {
    SET,
    LPUSH,
    RPUSH,
    DELETE,
    NONE
  }

  public abstract void add(byte[] key, byte[] value);

  public abstract void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException;

  public abstract int size();

  protected void wait(List<RedisFuture<?>> futures) throws InterruptedException {
    for (RedisFuture future: futures) {
      wait(future);
    }
  }

  protected void wait(RedisFuture<?> future) throws InterruptedException {
    log.debug("wait() - future = {}", future);
    if (!future.await(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS)) {
      future.cancel(true);
      throw new RetriableException(
          String.format("Timeout after %s ms while waiting for operation to complete.", this.config.operationTimeoutMs)
      );
    }
  }

  public static SinkOperation create(Type type, RedisSinkConnectorConfig config, int size) {
    SinkOperation result;

    switch (type) {
      case SET:
        result = new SetOperation(config, size);
        break;
      case LPUSH:
        result = new LPushOperation(config, size);
        break;
      case RPUSH:
        result = new RPushOperation(config, size);
        break;
      case DELETE:
        result = new DeleteOperation(config, size);
        break;
      default:
        throw new IllegalStateException(
            String.format("%s is not a supported operation.", type)
        );
    }

    return result;
  }

  static class NoneOperation extends SinkOperation {
    NoneOperation(RedisSinkConnectorConfig config) {
      super(Type.NONE, config);
    }

    @Override
    public void add(byte[] key, byte[] value) {
      throw new UnsupportedOperationException(
          "This should never be called."
      );
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) {

    }

    @Override
    public int size() {
      return 0;
    }
  }

  static class SetOperation extends SinkOperation {
    final Map<byte[], byte[]> sets;

    SetOperation(RedisSinkConnectorConfig config, int size) {
      super(Type.SET, config);
      this.sets = new LinkedHashMap<>(size);
    }

    @Override
    public void add(byte[] key, byte[] value) {
      this.sets.put(key, value);
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      log.debug("execute() - Calling mset with {} value(s)", this.sets.size());
      RedisFuture<?> future = asyncCommands.mset(this.sets);
      wait(future);
    }

    @Override
    public int size() {
      return this.sets.size();
    }
  }

  static abstract class PushOperation extends SinkOperation {

    final Map<byte[], List<byte[]>> pushes;

    PushOperation(RedisSinkConnectorConfig config, int size, Type type) {
      super(type, config);
      this.pushes = new LinkedHashMap<>(size);
    }

    @Override
    public void add(byte[] key, byte[] value) {
      List<byte[]> keyPushes = pushes.get(key);
      if (keyPushes == null) keyPushes = new ArrayList<>();
      keyPushes.add(value);
      this.pushes.put(key, keyPushes);
    }

    protected void execute(BiFunction<byte[], byte[][], RedisFuture<Long>> function) throws InterruptedException {
      log.debug("execute() - Calling {} with {} value(s)", this.type, this.pushes.size());
      List<RedisFuture<?>> futures = new ArrayList<>();
      for (Map.Entry<byte[], List<byte[]>> entry: pushes.entrySet()) {
        byte[][] values = entry.getValue().toArray(new byte[0][0]);
        futures.add(function.apply(entry.getKey(), values));
      }
      wait(futures);
    }

    @Override
    public int size() {
      return this.pushes.size();
    }
  }

  static class LPushOperation extends PushOperation {
    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      execute(asyncCommands::lpush);
    }
    LPushOperation(RedisSinkConnectorConfig config, int size) {
      super(config, size, Type.LPUSH);
    }
  }

  static class RPushOperation extends PushOperation {
    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      execute(asyncCommands::rpush);
    }
    RPushOperation(RedisSinkConnectorConfig config, int size) {
      super(config, size, Type.RPUSH);
    }
  }


  static class DeleteOperation extends SinkOperation {
    final List<byte[]> deletes;

    DeleteOperation(RedisSinkConnectorConfig config, int size) {
      super(Type.DELETE, config);
      this.deletes = new ArrayList<>(size);
    }

    @Override
    public void add(byte[] key, byte[] value) {
      this.deletes.add(key);
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      log.debug("execute() - Calling del with {} value(s)", this.deletes.size());
      byte[][] deletes = this.deletes.toArray(new byte[this.deletes.size()][]);
      RedisFuture<?> future = asyncCommands.del(deletes);
      wait(future);
    }

    @Override
    public int size() {
      return this.deletes.size();
    }
  }
}
