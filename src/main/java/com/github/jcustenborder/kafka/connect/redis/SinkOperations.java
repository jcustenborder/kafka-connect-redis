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

import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SinkOperations {
  private static final Logger log = LoggerFactory.getLogger(SinkOperations.class);
  final RedisConnectorConfig config;
  List<SinkOperation> operations = new ArrayList<>(1000);
  SinkOperation lastOperation;


  public SinkOperations(RedisConnectorConfig config) {
    this.config = config;
    this.lastOperation = new SinkOperation.None(this.config);
  }

  public void set(byte[] key, byte[] value) {
    SinkOperation.Set operation;
    if (SinkOperation.Type.SET == this.lastOperation.type) {
      operation = (SinkOperation.Set) this.lastOperation;
    } else {
      this.lastOperation = (operation = new SinkOperation.Set(this.config));
      this.operations.add(this.lastOperation);
    }
    operation.add(key, value);
  }

  public void del(byte[] key) {
    SinkOperation.Del operation;
    if (SinkOperation.Type.DEL == this.lastOperation.type) {
      operation = (SinkOperation.Del) this.lastOperation;
    } else {
      this.lastOperation = (operation = new SinkOperation.Del(this.config));
      this.operations.add(this.lastOperation);
    }
    operation.add(key);
  }

  public void hset(byte[] key, Map<byte[], byte[]> fieldValues) {
    SinkOperation.HSet operation;
    if (SinkOperation.Type.HSET == this.lastOperation.type) {
      operation = (SinkOperation.HSet) this.lastOperation;
    } else {
      this.lastOperation = (operation = new SinkOperation.HSet(this.config));
      this.operations.add(this.lastOperation);
    }
    operation.add(key, fieldValues);
  }

  public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) {
    for (SinkOperation sinkOperation : this.operations) {
      log.debug("execute() - operation = '{}'", sinkOperation);
      try {
        sinkOperation.execute(asyncCommands);
      } catch (InterruptedException e) {
        throw new RetriableException(e);
      }
    }
  }

  public void hdel(byte[] key) {
    SinkOperation.HDel operation;
    if (SinkOperation.Type.HDEL == this.lastOperation.type) {
      operation = (SinkOperation.HDel) this.lastOperation;
    } else {
      this.lastOperation = (operation = new SinkOperation.HDel(this.config));
      this.operations.add(this.lastOperation);
    }
    operation.add(key);
  }


  public void zrem(byte[] key, byte[] member) {
    SinkOperation.ZREM operation;
    if (SinkOperation.Type.ZREM == this.lastOperation.type) {
      operation = (SinkOperation.ZREM) this.lastOperation;
    } else {
      this.lastOperation = (operation = new SinkOperation.ZREM(this.config));
      this.operations.add(this.lastOperation);
    }
    operation.add(key, member);
  }

  public void geoadd(SinkOperation.GeoSetKey key, SinkOperation.Location location) {
    SinkOperation.GeoAdd operation;
    if (SinkOperation.Type.GEOADD == this.lastOperation.type) {
      operation = (SinkOperation.GeoAdd) this.lastOperation;
    } else {
      this.lastOperation = (operation = new SinkOperation.GeoAdd(this.config));
      this.operations.add(this.lastOperation);
    }
    operation.add(key, location);
  }

  public void srem(byte[] key, byte[] value) {
    SinkOperation.SREM operation;
    if (SinkOperation.Type.SREM == this.lastOperation.type) {
      operation = (SinkOperation.SREM) this.lastOperation;
    } else {
      this.lastOperation = (operation = new SinkOperation.SREM(this.config));
      this.operations.add(this.lastOperation);
    }
    operation.add(key, value);
  }

  public void sadd(byte[] key, byte[] value) {
    SinkOperation.SADD operation;
    if (SinkOperation.Type.SADD == this.lastOperation.type) {
      operation = (SinkOperation.SADD) this.lastOperation;
    } else {
      this.lastOperation = (operation = new SinkOperation.SADD(this.config));
      this.operations.add(this.lastOperation);
    }
    operation.add(key, value);
  }

  public void zrem(SinkOperation.GeoSetKey key) {
    zrem(key.key, key.member);
  }

  public void xadd(String key, Map<byte[], byte[]> mapValues) {
    SinkOperation.XADD operation;
    if (SinkOperation.Type.XADD == this.lastOperation.type) {
      operation = (SinkOperation.XADD) this.lastOperation;
    } else {
      this.lastOperation = (operation = new SinkOperation.XADD(this.config));
      this.operations.add(this.lastOperation);
    }
    operation.add(key, mapValues);
  }
}
