package com.fyp.engine.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);

    public static Properties getProperties(String propertiesName) {
        InputStream fis;
        Properties  properties = null;
        try {
            fis = PropertiesUtil.class.getResourceAsStream("/" + propertiesName + ".properties");
            if (fis != null) {
                properties = new Properties();
                properties.load(fis);
                fis.close();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return properties;
    }
}
