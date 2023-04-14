package com.fyp.engine.job.schema;

import com.alibaba.fastjson.JSON;
import com.fyp.engine.common.utils.JsonUtils;
import com.fyp.engine.job.message.ReceivedMessage;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class ReceivedMessageSchema implements SerializationSchema<ReceivedMessage>, KafkaDeserializationSchema<ReceivedMessage> {

    @Override
    public byte[] serialize(ReceivedMessage element) {
        return JsonUtils.toJsonString(element).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean isEndOfStream(ReceivedMessage nextElement) {
        return false;
    }

    @Override
    public ReceivedMessage deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String value = new String(record.value(), StandardCharsets.UTF_8);
        return JSON.parseObject(value, ReceivedMessage.class);
    }

    @Override
    public TypeInformation<ReceivedMessage> getProducedType() {
        return TypeInformation.of(new TypeHint<ReceivedMessage>() {
        });
    }
}