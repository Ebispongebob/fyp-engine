package com.fyp.engine.job;


import com.alibaba.fastjson.JSON;
import com.fyp.engine.common.exception.EngineException;
import com.fyp.engine.common.utils.HDFSSupport;
import com.fyp.engine.job.config.Configuration;

import java.io.IOException;

public class RtEngineEntryPoint {

    /**
     * 复杂规则任务入口
     * @param args 命令行参数
     * @throws IOException hdfs读取参数失败的时候抛出
     */
    public static void main(String[] args) throws IOException {
        // 任务配置解析
        Configuration configuration = parseCommandLine(args);

        // 任务配置校验
        check(configuration);

        // 构建
        RtEngine rtEngine = new AdpRtRuleEngine(configuration);

        // 运行
        rtEngine.run();
    }

    /**
     * 参数合法性前置校验
     * @param configuration 参数
     */
    private static void check(Configuration configuration) {
        // TODO: 前置参数校验
    }

    /**
     * 解析命令行参数为任务配置，参数支持Json字符串结构，索引0
     * 目前仅支持单任务
     * @param args 命令行参数
     * @return 任务配置
     */
    private static Configuration parseCommandLine(String[] args) throws IOException {
        if (args == null || args.length == 0) {
            throw new EngineException("命令行参数错误：args=" + args);
        }

        String str = args[0];
        // 直接传入json
        if (str.startsWith("{") && str.endsWith("}")) {
            return JSON.parseObject(str, Configuration.class);
        }
        // 从hdfs读取json数据
        return JSON.parseObject(HDFSSupport.readArgs(str), Configuration.class);
    }
}
