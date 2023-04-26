package com.fyp.engine.job.api;

import com.fyp.engine.job.message.EventMessage;
import com.fyp.engine.job.message.SinkMessage;
import com.fyp.engine.job.schema.EventMessageSchema;
import com.fyp.engine.job.schema.SinkMessageSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.List;
import java.util.Map;
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
    public static FlinkKafkaProducer<Map<String, List<SinkMessage>>> buildSinkMessageSink(String topic, String servers) {
        return new FlinkKafkaProducer<>(topic, new SinkMessageSchema(), buildProducerProps(servers));
    }

    private static Properties buildProducerProps(String servers) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        // leader接收到即发送成功
        properties.setProperty("acks", "1");
        // 每个分区批次: 1mb
        properties.setProperty("batch.size", "1048576");
        // 等待时间:100ms
        properties.setProperty("linger.ms", "100");
        // 缓存区大小:128mb
        properties.setProperty("buffer.memory", "134217728");
        return properties;
    }
}
