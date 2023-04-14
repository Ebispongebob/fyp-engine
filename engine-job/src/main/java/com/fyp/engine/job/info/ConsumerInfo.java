package com.fyp.engine.job.info;

import lombok.Data;

@Data
public class ConsumerInfo {

    /**
     * 订阅的mq消息
     */
    private String topic;

    /**
     * bootstrap servers
     */
    private String servers;

    /**
     * 分组ID，按照组进行消息分发
     */
    private String groupId;

    /**
     * 从某个时间开始消费，如果为0则从最新
     */
    private long startTime;
}
