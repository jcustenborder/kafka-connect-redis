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

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.hash.Hashing;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

abstract class SinkOperation {
  private static final Logger log = LoggerFactory.getLogger(SinkOperation.class);
  public final Type type;
  protected final RedisConnectorConfig config;

  SinkOperation(Type type, RedisConnectorConfig config) {
    this.type = type;
    this.config = config;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", type)
        .add("size", size())
        .toString();
  }

  public abstract void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException;

  public abstract int size();

  protected void wait(RedisFuture<?> future) throws InterruptedException {
    log.debug("wait() - future = {}", future);
    if (!future.await(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS)) {
      future.cancel(true);
      throw new RetriableException(
          String.format("Timeout after %s ms while waiting for operation to complete.", this.config.operationTimeoutMs)
      );
    }
  }

  public enum Type {
    SET,
    HSET,
    HDEL,
    GEOADD,
    DEL,
    ZREM,
    SADD,
    SREM,
    XADD, NONE
  }

  static class None extends SinkOperation {
    None(RedisConnectorConfig config) {
      super(Type.NONE, config);
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) {

    }

    @Override
    public int size() {
      return 0;
    }
  }

  static class Set extends SinkOperation {
    final Map<byte[], byte[]> sets;

    Set(RedisConnectorConfig config) {
      super(Type.SET, config);
      this.sets = new LinkedHashMap<>();
    }

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

  static class Del extends SinkOperation {
    final List<byte[]> deletes;

    Del(RedisConnectorConfig config) {
      super(Type.DEL, config);
      this.deletes = new ArrayList<>();
    }

    public void add(byte[] key) {
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

  static class HSet extends SinkOperation {
    Map<byte[], Map<byte[], byte[]>> operations;

    public HSet(RedisConnectorConfig config) {
      super(Type.HSET, config);
      this.operations = new LinkedHashMap<>();
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      RedisFuture<Boolean>[] futures = this.operations.entrySet().stream()
          .flatMap(new FlatMapper(asyncCommands))
          .toArray(RedisFuture[]::new);
      log.trace("execute() - Waiting on {} operation(s).", futures.length);
      LettuceFutures.awaitAll(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS, futures);
    }

    public void add(byte[] key, Map<byte[], byte[]> fieldValues) {
      this.operations.put(key, fieldValues);
    }

    @Override
    public int size() {
      return this.operations.size();
    }

    static class FlatMapper implements Function<Map.Entry<byte[], Map<byte[], byte[]>>, Stream<RedisFuture<Boolean>>> {
      final RedisClusterAsyncCommands<byte[], byte[]> asyncCommands;

      FlatMapper(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) {
        this.asyncCommands = asyncCommands;
      }

      @Override
      public Stream<RedisFuture<Boolean>> apply(Map.Entry<byte[], Map<byte[], byte[]>> mapEntry) {
        return mapEntry.getValue().entrySet()
            .stream()
            .map(e -> asyncCommands.hset(mapEntry.getKey(), e.getKey(), e.getValue()));
      }
    }
  }

  static class HDel extends SinkOperation {
    List<byte[]> operations;

    public HDel(RedisConnectorConfig config) {
      super(Type.HDEL, config);
      this.operations = new ArrayList<>(100);
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      List<RedisFuture<?>> hdels = new ArrayList<>();
      for (byte[] key : this.operations) {
        RedisFuture<List<byte[]>> result = asyncCommands.hkeys(key);
        List<byte[]> keys = LettuceFutures.awaitOrCancel(result, this.config.operationTimeoutMs, TimeUnit.MILLISECONDS);
        RedisFuture<Long> foo = asyncCommands.hdel(key, keys.toArray(new byte[0][]));
        hdels.add(foo);
      }
      LettuceFutures.awaitAll(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS, hdels.toArray(new RedisFuture[0]));
    }

    @Override
    public int size() {
      return this.operations.size();
    }

    public void add(byte[] key) {
      this.operations.add(key);
    }
  }

  static class GeoSetKey {
    final byte[] key;
    final byte[] member;
    final int hashCode;

    private GeoSetKey(byte[] key, byte[] member) {
      this.key = key;
      this.member = member;
      this.hashCode = Hashing.goodFastHash(64)
          .hashBytes(this.key)
          .asInt();
    }

    public static GeoSetKey of(byte[] key, byte[] member) {
      return new GeoSetKey(key, member);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(this.hashCode);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GeoSetKey geoSetKey = (GeoSetKey) o;
      return (this.hashCode == geoSetKey.hashCode);
    }
  }

  static class Location {
    final double longitude;
    final double latitude;
    final byte[] member;

    Location(double longitude, double latitude, byte[] member) {
      this.longitude = longitude;
      this.latitude = latitude;
      this.member = member;
    }

    public static Location of(double longitude, double latitude, byte[] member) {
      return new Location(longitude, latitude, member);
    }

    public Stream<Object> items() {
      return Stream.of(this.latitude, this.longitude, this.member);
    }
  }


  static class GeoAdd extends SinkOperation {
    private final ListMultimap<GeoSetKey, Location> operations;


    public GeoAdd(RedisConnectorConfig config) {
      super(Type.GEOADD, config);
      this.operations = LinkedListMultimap.create();
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      List<RedisFuture<?>> results = new ArrayList<>();
      for (GeoSetKey key : this.operations.keySet()) {
        Collection<Location> locations = this.operations.get(key);
        Object[] values = locations.stream()
            .flatMap(Location::items)
            .toArray(Object[]::new);
        results.add(
            asyncCommands.geoadd(key.key, values)
        );
      }

      if (!LettuceFutures.awaitAll(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS, results.toArray(new RedisFuture<?>[0]))) {
        throw new IllegalStateException();
      }

    }

    public void add(GeoSetKey key, Location location) {
      this.operations.put(key, location);
    }

    @Override
    public int size() {
      return this.operations.size();
    }
  }

  static class ZREM extends SinkOperation {
    public final Map<byte[], byte[]> operations;

    public ZREM(RedisConnectorConfig config) {
      super(Type.ZREM, config);
      this.operations = new LinkedHashMap<>();
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      log.debug("execute() - Calling srem with {} value(s)", this.operations.size());
      RedisFuture<?>[] results = this.operations.entrySet()
          .stream()
          .map(e -> asyncCommands.zrem(e.getKey(), e.getValue()))
          .toArray(RedisFuture[]::new);
      if (!LettuceFutures.awaitAll(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS, results)) {
        throw new IllegalStateException();
      }
    }

    @Override
    public int size() {
      return this.operations.size();
    }

    public void add(byte[] key, byte[] member) {
      this.operations.put(key, member);
    }
  }

  public static class SREM extends SinkOperation {
    private final Map<byte[], byte[]> operations;

    public SREM(RedisConnectorConfig config) {
      super(Type.SREM, config);
      this.operations = new LinkedHashMap<>();
    }

    public void add(byte[] key, byte[] value) {
      this.operations.put(key, value);
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      log.debug("execute() - Calling srem with {} value(s)", this.operations.size());
      RedisFuture[] results = this.operations.entrySet()
          .stream()
          .map(e -> asyncCommands.srem(e.getKey(), e.getValue()))
          .toArray(RedisFuture[]::new);
      if (!LettuceFutures.awaitAll(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS, results)) {
        throw new IllegalStateException();
      }
    }

    @Override
    public int size() {
      return this.operations.size();
    }
  }

  public static class SADD extends SinkOperation {
    private final Map<byte[], byte[]> operations;

    public SADD(RedisConnectorConfig config) {
      super(Type.SADD, config);
      this.operations = new LinkedHashMap<>();
    }

    public void add(byte[] key, byte[] value) {
      this.operations.put(key, value);
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      log.debug("execute() - Calling sadd with {} value(s)", this.operations.size());
      RedisFuture[] results = this.operations.entrySet()
          .stream()
          .map(e -> asyncCommands.sadd(e.getKey(), e.getValue()))
          .toArray(RedisFuture[]::new);
      if (!LettuceFutures.awaitAll(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS, results)) {
        throw new IllegalStateException();
      }
    }

    @Override
    public int size() {
      return this.operations.size();
    }
  }

  public static class XADD extends SinkOperation {
    private final ListMultimap<String, Map<byte[], byte[]>> operations;

    public XADD(RedisConnectorConfig config) {
      super(Type.XADD, config);
      this.operations = LinkedListMultimap.create();
    }

    public void add(String key, Map<byte[], byte[]> value) {
      this.operations.put(key, value);
    }

    @Override
    public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
      log.debug("execute() - Calling xadd with {} value(s)", this.operations.size());
      List<RedisFuture<?>> results = new ArrayList<>();
      for (String topic : this.operations.keySet()) {
        byte[] key = topic.getBytes(this.config.charset);
        List<Map<byte[], byte[]>> bodies = this.operations.get(topic);
        bodies.forEach(body -> {
          results.add(
              asyncCommands.xadd(key, body)
          );
        });
      }

      if (!LettuceFutures.awaitAll(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS, results.toArray(new RedisFuture<?>[0]))) {
        throw new IllegalStateException();
      }
    }

    @Override
    public int size() {
      return this.operations.size();
    }
  }
}
