package com.github.jcustenborder.kafka.connect.redis;

import com.google.common.net.HostAndPort;
import io.lettuce.core.RedisFuture;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RedisSinkConnectorConfigTest {

    private Map<String, String> props;
    private final Logger log = LoggerFactory.getLogger(RedisSinkConnectorConfigTest.class);

    @BeforeEach
    public void setup() {
        props = new HashMap<>();
    }

    @Test
    public void testDefaultOperationTimeoutsConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.operationTimeoutMs, 10000L);
    }

    @Test
    public void testSetOperationTimeoutConfig() {
        props.put(RedisSinkConnectorConfig.OPERATION_TIMEOUT_MS_CONF, "33000");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.operationTimeoutMs, 33000L);
    }

    @Test
    public void testInvalidOperationTimeoutConfig() {
        props.put(RedisSinkConnectorConfig.OPERATION_TIMEOUT_MS_CONF, "99");
        Assertions.assertThrows(ConfigException.class, () -> {
            new RedisSinkConnectorConfig(props);
        });
    }

    @Test
    public void testDefaultCharsetConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.charset.toString(), "UTF-8");
    }

    @Test
    public void testSetCharsetConfig() {
        props.put(RedisSinkConnectorConfig.CHARSET_CONF, "windows-1257");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.charset.toString(), "windows-1257");
    }

    @Test
    public void testInvalidCharsetConfig() {
        props.put(RedisSinkConnectorConfig.CHARSET_CONF, "Big56");
        Assertions.assertThrows(ConfigException.class, () -> {
            new RedisSinkConnectorConfig(props);
        });
    }

    @Test
    public void testSSLConfigs() {
        props.put(RedisSinkConnectorConfig.SSL_CONFIG, "true");
        props.put(RedisSinkConnectorConfig.SSL_PROVIDER_CONFIG, "OPENSSL");
        props.put(RedisSinkConnectorConfig.SSL_KEYSTORE_PASSWORD_CONFIG, "pass1234");
        props.put(RedisSinkConnectorConfig.SSL_KEYSTORE_PATH_CONFIG, "/path1");
        props.put(RedisSinkConnectorConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG, "pass4321");
        props.put(RedisSinkConnectorConfig.SSL_TRUSTSTORE_PATH_CONFIG, "/path2");

        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);

        assertTrue(config.sslEnabled);
        assertEquals(config.sslProvider, RedisConnectorConfig.RedisSslProvider.OPENSSL);
        assertEquals(config.keystorePassword, "pass1234");
        assertEquals(config.keystorePath, new File("/path1"));
        assertEquals(config.truststorePassword, "pass4321");
        assertEquals(config.truststorePath, new File("/path2"));
    }

    @Test
    public void testDefaultHostConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        List<HostAndPort> result = new ArrayList<>();
        HostAndPort hostAndPort = HostAndPort.fromString("localhost:6379");
        result.add(hostAndPort);
        assertEquals(config.hosts, result);
    }

    @Test
    public void testSetHostsConfig() {
        props.put(RedisSinkConnectorConfig.HOSTS_CONFIG, "127.4.5.7:6345,152.4.3.2");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        List<HostAndPort> result = new ArrayList<>();
        HostAndPort hostAndPort1 = HostAndPort.fromString("127.4.5.7:6345");
        HostAndPort hostAndPort2 = HostAndPort.fromString("152.4.3.2:6379");
        result.add(hostAndPort1);
        result.add(hostAndPort2);
        assertEquals(config.hosts, result);
    }

    @Test
    public void testInvalidHostsConfig() {
        props.put(RedisSinkConnectorConfig.HOSTS_CONFIG, "333.3.2.1:66666");
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new RedisSinkConnectorConfig(props);
        });
    }

    @Test
    public void testDefaultPasswordConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.password, "");
    }

    @Test
    public void testSetPasswordConfig() {
        props.put(RedisSinkConnectorConfig.PASSWORD_CONFIG, "hocuspocus");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.password, "hocuspocus");
    }

    @Test
    public void testDefaultDatabaseConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.database, 1);
    }

    @Test
    public void testSetDatabaseConfig() {
        props.put(RedisSinkConnectorConfig.DATABASE_CONFIG, "4");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.database, 4);
    }

    @Test
    public void testDefaultClientModeConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.clientMode, RedisSinkConnectorConfig.ClientMode.Standalone);
    }

    @Test
    public void testSetClientModeConfig() {
        props.put(RedisSinkConnectorConfig.CLIENT_MODE_CONFIG, "Cluster");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.clientMode, RedisSinkConnectorConfig.ClientMode.Cluster);
    }

    @Test
    public void testDefaultAutoReconnectEnabledConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.autoReconnectEnabled, true);
    }

    @Test
    public void testSetAutoReconnectEnabledConfig() {
        props.put(RedisSinkConnectorConfig.AUTO_RECONNECT_ENABLED_CONFIG, "false");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.autoReconnectEnabled, false);
    }

    @Test
    public void testInvalidAutoReconnectEnabledConfig() {
        props.put(RedisSinkConnectorConfig.AUTO_RECONNECT_ENABLED_CONFIG, "99");
        Assertions.assertThrows(ConfigException.class, () -> {
            new RedisSinkConnectorConfig(props);
        });
    }

    @Test
    public void testDefaultRequestQueueSizeConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.requestQueueSize, 2147483647);
    }

    @Test
    public void testSetRequestQueueSizeConfig() {
        props.put(RedisSinkConnectorConfig.REQUEST_QUEUE_SIZE_CONFIG, "456734");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.requestQueueSize, 456734);
    }

    @Test
    public void testInvalidRequestQueueSizeConfig() {
        props.put(RedisSinkConnectorConfig.REQUEST_QUEUE_SIZE_CONFIG, "hello");
        Assertions.assertThrows(ConfigException.class, () -> {
            new RedisSinkConnectorConfig(props);
        });
    }

    @Test
    public void testDefaultSocketTCPNoDelayConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.tcpNoDelay, true);
    }

    @Test
    public void testSetSocketTCPNoDelayConfig() {
        props.put(RedisSinkConnectorConfig.SOCKET_TCP_NO_DELAY_CONFIG, "false");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.tcpNoDelay, false);
    }

    @Test
    public void testInvalidSocketTCPNoDelayConfig() {
        props.put(RedisSinkConnectorConfig.SOCKET_TCP_NO_DELAY_CONFIG, "99");
        Assertions.assertThrows(ConfigException.class, () -> {
            new RedisSinkConnectorConfig(props);
        });
    }

    @Test
    public void testDefaultSocketKeepAliveConfig() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.keepAliveEnabled, false);
    }

    @Test
    public void testSetSocketKeepAliveConfig() {
        props.put(RedisSinkConnectorConfig.SOCKET_KEEP_ALIVE_CONFIG, "true");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.keepAliveEnabled, true);
    }

    @Test
    public void testInvalidSocketKeepAliveConfig() {
        props.put(RedisSinkConnectorConfig.SOCKET_KEEP_ALIVE_CONFIG, "99");
        Assertions.assertThrows(ConfigException.class, () -> {
            new RedisSinkConnectorConfig(props);
        });
    }

    @Test
    public void testRedisActionConfigDefaultsToSet() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.redisAction, "set");
    }

    @Test
    public void testRedisActionConfigReceivesPublishAction() {
        props.put(RedisSinkConnectorConfig.REDIS_ACTION_CONF, "publish");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.redisAction, "publish");
    }

    @Test
    public void testRedisActionConfigReceivesSetAction() {
        props.put(RedisSinkConnectorConfig.REDIS_ACTION_CONF, "set");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.redisAction, "set");
    }

    @Test
    public void testInvalidRedisActionConfigThrowsConfigException() {
        props.put(RedisSinkConnectorConfig.REDIS_ACTION_CONF, "invalid");
        Assertions.assertThrows(ConfigException.class, () -> {
            new RedisSinkConnectorConfig(props);
        });
    }

    @Test
    public void testRedisChannelArgumentDefaultsToNull() {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.redisChannel, "");
    }

    @Test
    public void testRedisChannelArgumentReceivesStringValue() {
        props.put(RedisSinkConnectorConfig.REDIS_CHANNEL_CONF, "my-channel");
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        assertEquals(config.redisChannel, "my-channel");
    }

    @Test
    public void testRedisChannelArgumentReceivesStringValueAndPublishesMessage() throws InterruptedException {
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        RedisSession sesh = new RedisSessionFactoryImpl().create(config);
        Map<byte[],  byte[]> redisVals = new HashMap<>();
        redisVals.put("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
        RedisFuture<String> res = sesh.asyncCommands().mset(redisVals);
        res.wait();
    }
}
