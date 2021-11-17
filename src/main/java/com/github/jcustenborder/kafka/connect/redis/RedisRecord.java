package com.github.jcustenborder.kafka.connect.redis;
//
//import org.apache.kafka.connect.errors.DataException;
//import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Charsets;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisRecord {
    private static final Logger log = LoggerFactory.getLogger(RedisRecord.class);

    private final byte[] key;
    private final byte[] value;

    private RedisRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] key() {
        return this.key;
    }

    public byte[] value() {
        return this.value;
    }


    public static RedisRecord fromDeleteRecord(SinkRecord deleteRecord, RedisSinkConnectorConfig config) throws DataException {
        return new RedisRecord(keyFromRecord(deleteRecord, config), null);
    }

    public static RedisRecord fromSetRecord(SinkRecord setRecord, RedisSinkConnectorConfig config) {
        return new RedisRecord(keyFromRecord(setRecord, config), toBytes("value", setRecord.value(), config));
    }

    public static RedisRecord fromSinkOffsetState(SinkOffsetState e) {
        final byte[] key = String.format("__kafka.offset.%s.%s", e.topic(), e.partition()).getBytes(Charsets.UTF_8);
        final byte[] value;
        try {
            value = ObjectMapperFactory.INSTANCE.writeValueAsBytes(e);
        } catch (JsonProcessingException e1) {
            throw new DataException(e1);
        }
        log.trace("RedisRecord::fromSinkOffsetState: Setting offset: {}", e);
        return new RedisRecord(key, value);
    }

    private static byte[] keyFromRecord(SinkRecord record, RedisSinkConnectorConfig config) {
        log.trace("RedisRecord::keyFromRecord - Processing record " + formatLocation(record));
        if (record.key() == null) {
            throw new DataException(
                    "The key for the record cannot be null. " + formatLocation(record)
            );
        }
        final byte[] key = toBytes("key", record.key(), config);
        if (null == key || key.length == 0) {
            throw new DataException(
                    "The key cannot be an empty byte array. " + formatLocation(record)
            );
        }
        return key;
    }

    private static byte[] toBytes(String source, Object input, RedisSinkConnectorConfig config) {
        final byte[] result;

        if (input instanceof String) {
            String s = (String) input;
            result = s.getBytes(config.charset);
        } else if (input instanceof byte[]) {
            result = (byte[]) input;
        } else if (null == input) {
            result = null;
        } else {
            throw new DataException(
                    String.format(
                            "The %s for the record must be String or Bytes. Consider using the ByteArrayConverter " +
                                    "or StringConverter if the data is stored in Kafka in the format needed in Redis. " +
                                    "Another option is to use a single message transformation to transform the data before " +
                                    "it is written to Redis.",
                            source
                    )
            );
        }

        return result;
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
