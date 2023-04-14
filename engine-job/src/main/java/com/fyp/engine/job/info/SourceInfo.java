package com.fyp.engine.job.info;

import lombok.Data;

@Data
public class SourceInfo {

    /**
     * 数据源算子的并行度，默认为1
     */
    private int parallelism;

    /**
     * 水位，固定延迟策略，单位：ms，默认10秒
     */
    private long watermark;

    /**
     * 消费者配置
     */
    private ConsumerInfo consumerInfo;
}
