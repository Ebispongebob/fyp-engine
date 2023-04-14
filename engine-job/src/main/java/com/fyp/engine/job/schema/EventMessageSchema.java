package com.fyp.engine.job.schema;


import com.alibaba.fastjson.JSON;
import com.fyp.engine.common.utils.JsonUtils;
import com.fyp.engine.job.message.EventMessage;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class EventMessageSchema implements SerializationSchema<EventMessage>, KafkaDeserializationSchema<EventMessage> {

    @Override
    public byte[] serialize(EventMessage element) {
        return JsonUtils.toJsonString(element).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean isEndOfStream(EventMessage nextElement) {
        return false;
    }

    @Override
    public EventMessage deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String value = new String(record.value(), StandardCharsets.UTF_8);
        return JSON.parseObject(value, EventMessage.class);
    }

    @Override
    public TypeInformation<EventMessage> getProducedType() {
        return TypeInformation.of(new TypeHint<EventMessage>() {
        });
    }
}