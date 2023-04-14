package com.fyp.engine.job.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 配置文件工具类.
 * @title CommonConfigUtils
 * @description 支持读取多个配置文件的内容读取.
 * @author hanzhiwei
 * @date 2016年10月20日
 * @version 1.0
 */
public class CommonConfigUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonConfigUtils.class);

    private static Map<String, Configuration> configMap = new ConcurrentHashMap();

    private CommonConfigUtils() {
    }

    /**
     * 获取指定配置文件对应的configuration对象
     * @param configFilename 配置文件
     * @return config 对象，如果配置文件不存在或者读取异常则返回null.
     */
    public static Configuration getConfig(String configFilename) {
        return getConfig(configFilename, CommonConfigUtils.class.getClassLoader());
    }

    /**
     * 通过指定的classloader获取配置文件对应的configuration对象
     * @param configFilename
     * @param classLoader
     * @return
     */
    public static Configuration getConfig(String configFilename, ClassLoader classLoader) {
        if (configMap.get(configFilename) != null) {
            return configMap.get(configFilename);
        }
        synchronized (CommonConfigUtils.class) {
            if (configMap.get(configFilename) != null) {
                return configMap.get(configFilename);
            }
            try {
                URL           url    = classLoader.getResource(configFilename);
                Configuration config = new PropertiesConfiguration(url);
                configMap.put(configFilename, config);
                return config;
            } catch (ConfigurationException e) {
                LOGGER.error("get config with " + configFilename + " failed.", e);
                return null;
            }
        }

    }
}