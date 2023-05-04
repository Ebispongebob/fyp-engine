package com.fyp.engine.job;

import com.fyp.engine.common.exception.EngineException;
import com.fyp.engine.job.config.Configuration;
import com.fyp.engine.job.info.MatchInfo;
import com.fyp.engine.job.info.SinkInfo;
import com.fyp.engine.job.info.SourceInfo;
import com.fyp.engine.job.message.SinkMessage;
import com.fyp.engine.job.utils.CommonConfigUtils;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public abstract class BaseRtEngine<T> implements RtEngine {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected static org.apache.commons.configuration.Configuration config = CommonConfigUtils.getConfig("application.properties");

    /**
     * fs根目录
     */
    protected static final String CHECKPOINT_PRE = "hdfs:///flink/nighthawk/cep/checkpoints";

    /**
     * 配置对象
     */
    protected Configuration configuration;

    /**
     * 执行环境
     */
    private StreamExecutionEnvironment env;

    public BaseRtEngine(Configuration configuration) {
        if (configuration == null) {
            throw new EngineException("配置类不能为null");
        }

        this.configuration = configuration;

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 初始化flink环境配置
        initEnv(env, configuration);

        // 初始化输入数据源
        DataStream<T> source = initSource(env, configuration.getSourceInfo());

        logger.error("source ok");

        // 初始化匹配处理逻辑
        DataStream<Map<String, List<SinkMessage>>> match = initMatcher(env, configuration.getMatchInfo(), source);
        logger.error("match ok");

        // 初始化输出目的地
        initSink(env, configuration.getSinkInfo(), match);
        logger.error("sink ok");
    }

    /**
     * 添加输出流
     * @param env 环境
     * @param sinkInfo 输出配置
     * @param filterStream 处理流
     */
    protected abstract void initSink(StreamExecutionEnvironment env, SinkInfo sinkInfo, DataStream<Map<String, List<SinkMessage>>> filterStream);

    /**
     * 初始化处理流
     * @param env 环境
     * @param matchInfo 配置
     * @param sourceStream 数据源流
     * @return 匹配流、超时流二选一
     */
    protected abstract DataStream<Map<String, List<SinkMessage>>> initMatcher(StreamExecutionEnvironment env, MatchInfo matchInfo,
            DataStream<T> sourceStream);

    /**
     * 初始化输入流
     * @param env 环境
     * @param sourceInfo 输入配置
     * @return 输入流
     */
    protected abstract DataStream<T> initSource(StreamExecutionEnvironment env, SourceInfo sourceInfo);

    /**
     * 初始化执行环境
     * @param env 环境对象
     * @param configuration 配置
     */
    protected void initEnv(StreamExecutionEnvironment env, Configuration configuration) {
        // watermark的调度周期与watermark频率保持一致
        long watermark = getWatermark(configuration.getSourceInfo());
        // 必须大于默认的200毫秒频率
        final long defaultWaterMark = 200L;
        if (watermark > defaultWaterMark) {
            env.getConfig().setAutoWatermarkInterval(watermark);
        }

        // 开启嵌入式rocksDb
        if (enableRocksDb(configuration)) {
            // 默认增量
            EmbeddedRocksDBStateBackend rocksDB = new EmbeddedRocksDBStateBackend(true);
            // 采用预定义的针对固态硬盘的参数配置
            rocksDB.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
            env.setStateBackend(rocksDB);
            logger.info("开启rocksDb状态后端");
        }

        // 自定义checkpoint配置
        if (enableCheckpointConfig()) {
            configureCheckpoint(env, configuration);
        }
        if (null != configuration.getProps()) {
            Map<String, String> params = Maps.newHashMap();
            params.putAll(env.getConfig().getGlobalJobParameters().toMap());
            env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(params));
        }
    }

    /**
     * 是否开启自定义checkpoint配置，默认关闭
     * @return true：是；false：不开启
     */
    protected boolean enableCheckpointConfig() {
        return false;
    }

    /**
     * 程序checkpoint自定义
     * @param env 环境
     * @param configuration 配置
     */
    private void configureCheckpoint(StreamExecutionEnvironment env, Configuration configuration) {
        // 开启checkpoint，默认10秒checkpoint一次
        long checkpointInterval = configuration.getCheckpointInterval() > 0 ? configuration.getCheckpointInterval() : 10000;
        env.enableCheckpointing(checkpointInterval);
        // 精准一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 最小等待等于checkpoint周期
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointInterval);
        // 默认10分钟超时
        env.getCheckpointConfig().setCheckpointTimeout(configuration.getCheckpointTimeout() > 0 ? configuration.getCheckpointTimeout() : 600000);
        // 任务取消时保留状态
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // hdfs持久化checkpoint
        if (enableFsCheckpointStorage(configuration)) {
            String directory = buildDirectory(CHECKPOINT_PRE, configuration);
            env.getCheckpointConfig().setCheckpointStorage(directory);
            logger.info("fs存储地址：{}", directory);
        }
    }

    /**
     * 获取watermark的时间戳，默认10秒
     * @param sourceInfo 数据源
     * @return 时间戳
     */
    protected long getWatermark(SourceInfo sourceInfo) {
        return sourceInfo.getWatermark() > 0 ? (int) sourceInfo.getWatermark() : 10000;
    }

    /**
     * 开启hdfs持久化
     * @param configuration 配置
     * @return true：开启；false：不开启
     */
    protected boolean enableFsCheckpointStorage(Configuration configuration) {
        return configuration.getFsCheckpointStorage() != null && configuration.getFsCheckpointStorage();
    }

    /**
     * 开启rocksdb
     * @param configuration 配置
     * @return true：开启；false：不开启
     */
    protected boolean enableRocksDb(Configuration configuration) {
        return configuration.getRocksDb() != null && configuration.getRocksDb();
    }

    /**
     * 生成checkpoint完整路径
     * @param root 根路径
     * @param configuration 配置
     * @return 完整路径
     */
    private String buildDirectory(String root, Configuration configuration) {
        String datetime = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").format(LocalDateTime.now());
        return root + datetime;
    }

    @Override
    public void run() {
        if (env == null) {
            throw new EngineException("执行环境未配置");
        }

        try {
            logger.info("执行计划打印：{}", env.getExecutionPlan());

            env.execute(configuration.getJobName());

            logger.info("RtEngine开始运行");
        } catch (Throwable e) {
            throw new EngineException("RtEngine开始异常：", e);
        }
    }
}
