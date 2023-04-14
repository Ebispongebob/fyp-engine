package com.fyp.engine.job.config;

import com.fyp.engine.job.info.MatchInfo;
import com.fyp.engine.job.info.SinkInfo;
import com.fyp.engine.job.info.SourceInfo;
import lombok.Data;

import java.util.Map;

@Data
public class Configuration {
    /**
     * 任务名称
     */
    private String jobName;

    /**
     * 开启rocksDb state backend
     */
    private Boolean rocksDb;

    /**
     * 开启fs的持久化
     */
    private Boolean fsCheckpointStorage;

    /**
     * checkpoint发送周期，默认：10000毫秒
     */
    private long checkpointInterval;

    /**
     * 默认10分钟
     */
    private long checkpointTimeout;

    /**
     * 输入源配置
     */
    private SourceInfo sourceInfo;

    /**
     * 处理器配置
     */
    private MatchInfo matchInfo;

    /**
     * 输出地配置
     */
    private SinkInfo sinkInfo;

    /**
     * 其他配置
     */
    private Map<String, Object> props;

}
