package com.github.jcustenborder.kafka.connect.redis;

import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.BeforeEach;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractTaskTest<TASK extends Task> {
  protected RedisSession session;
  protected RedisGeoAsyncCommands<byte[], byte[]> geo;
  protected RedisHashAsyncCommands<byte[], byte[]> hash;
  protected RedisStringAsyncCommands<byte[], byte[]> string;
  protected RedisKeyAsyncCommands<byte[], byte[]> key;
  protected RedisSortedSetAsyncCommands<byte[], byte[]> sortedSet;
  protected RedisStreamAsyncCommands<byte[], byte[]> streams;

  protected TASK task;

  protected abstract TASK createTask();

  protected RedisSessionFactory sessionFactory;
  protected Map<String, String> settings;

  @BeforeEach
  public void setupSession() {
    this.settings = new LinkedHashMap<>();
    
    this.session = mock(RedisSession.class);
    this.geo = mock(RedisGeoAsyncCommands.class);
    this.hash = mock(RedisHashAsyncCommands.class);
    this.string = mock(RedisStringAsyncCommands.class);
    this.key = mock(RedisKeyAsyncCommands.class);
    this.sortedSet = mock(RedisSortedSetAsyncCommands.class);
    this.streams = mock(RedisStreamAsyncCommands.class);

    when(this.session.geo()).thenReturn(this.geo);
    when(this.session.hash()).thenReturn(this.hash);
    when(this.session.string()).thenReturn(this.string);
    when(this.session.key()).thenReturn(this.key);
    when(this.session.sortedSet()).thenReturn(this.sortedSet);
    when(this.session.streams()).thenReturn(this.streams);
    
    this.task = createTask();
    
    this.sessionFactory = mock(RedisSessionFactory.class);
    when(this.sessionFactory.createSession(any())).thenReturn(this.session);
    
    

  }


}
