package com.fyp.engine.job.info;

import lombok.Data;

@Data
public class SinkInfo {
    /**
     * 订阅的mq消息
     */
    private String topic;

    /**
     * bootstrap servers
     */
    private String servers;
    /**
     * sink type
     */
    private String sinkType;
}
