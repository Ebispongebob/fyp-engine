package com.fyp.engine.job.schema;

import com.alibaba.fastjson.JSON;
import com.fyp.engine.common.utils.JsonUtils;
import com.fyp.engine.job.message.ReceivedMessage;
import com.fyp.engine.job.message.SinkMessage;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class SinkMessageSchema implements SerializationSchema<Map<String, List<SinkMessage>>>, KafkaDeserializationSchema<Map<String, List<SinkMessage>>> {

    @Override
    public byte[] serialize(Map<String, List<SinkMessage>> element) {
        return JsonUtils.toJsonString(element).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean isEndOfStream(Map<String, List<SinkMessage>> nextElement) {
        return false;
    }

    @Override
    public Map<String, List<SinkMessage>> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String value = new String(record.value(), StandardCharsets.UTF_8);
        return JSON.parseObject(value, Map.class);
    }

    @Override
    public TypeInformation<Map<String, List<SinkMessage>>> getProducedType() {
        return TypeInformation.of(new TypeHint<Map<String, List<SinkMessage>>>() {
        });
    }
}