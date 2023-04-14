package com.fyp.engine.job.api;

import com.fyp.engine.job.message.EventMessage;
import com.fyp.engine.job.message.ReceivedMessage;
import com.fyp.engine.job.schema.EventMessageSchema;
import com.fyp.engine.job.schema.ReceivedMessageSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaFactory {

    /**
     * 构建kafka消费者的配置参数
     * @param groupId 消费者分组
     * @param servers 服务器地址集
     * @return 配置
     */
    public static Properties buildConsumerbProperties(String groupId, String servers) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.put("group.id", groupId);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return properties;
    }

    /**
     * 构建kafka生产者的配置参数
     * @param servers 服务器地址集
     * @return 配置
     */
    public static Properties buildProperties(String servers) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return properties;
    }

    /**
     * 构建kafka source
     * @param topic 话题
     * @param servers 服务器集群地址
     * @param groupId 消费者分组
     * @return 消费者
     */
    public static FlinkKafkaConsumer<EventMessage> buildEventMsgSource(String topic, String servers, String groupId) {
        return new FlinkKafkaConsumer<>(topic, new EventMessageSchema(), buildConsumerbProperties(groupId, servers));
    }

    /**
     * 构建kafka sink
     * @param topic 话题
     * @param servers 服务器集群地址
     * @return 生产者
     */
    public static KafkaSink<ReceivedMessage> buildReceivedMsgSink(String topic, String servers) {
        return KafkaSink.<ReceivedMessage>builder()
                        .setBootstrapServers(servers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                                                           .setTopic(topic)
                                                                           .setValueSerializationSchema(new ReceivedMessageSchema())
                                                                           .build()
                        )
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build();
    }
}
