package com.fyp.engine.common.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorUtils {
    /**
     * 计算合适的并行度
     * 不高于任务配置的并行度
     * @param env 环境
     * @param parallelism 并行度
     * @return 合适的并行度
     */
    public static int calParallelism(StreamExecutionEnvironment env, int parallelism) {
        return Math.min(Math.max(parallelism, 1), Math.max(env.getParallelism(), 1));
    }

    /**
     * 通过比例计算合适的并行度
     * @param env 环境
     * @param parallelismRatio 并行度比例
     * @return 合适的并行度
     */
    public static int calParallelism(StreamExecutionEnvironment env, double parallelismRatio) {
        int parallelism = (int) Math.round(env.getParallelism() * parallelismRatio);
        return calParallelism(env, parallelism);
    }

}
