package com.fyp.engine.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

public class JsonUtils {

    public static String toJsonString(Object obj) {
        if (obj == null) {
            return null;
        }
        // 包括null值，日期格式化
        return JSON.toJSONString(obj, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteDateUseDateFormat);
    }

    public static JSONObject toJsonObject(String json) {
        return JSON.parseObject(json);
    }

    public static JSONArray toJsonArray(String json) {
        return JSON.parseArray(json);
    }

    public static <T> List<T> toJsonArray(String json, Class<T> cls) {
        return JSON.parseArray(json, cls);
    }

    public static JSONObject toJsonObject(Object obj) {
        if (obj == null) {
            return null;
        }

        String json = toJsonString(obj);
        return JSON.parseObject(json);
    }

    public static <T> T parse(String json, Class<T> cls) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        return JSON.parseObject(json, cls);
    }

    public static <T> T parse(Object obj, Class<T> cls) {
        if (obj == null) {
            return null;
        }
        String json = toJsonString(obj);
        return parse(json, cls);
    }

    public static <T> T parse(String text, TypeReference<T> type) {
        if (StringUtils.isBlank(text)) {
            return null;
        }

        return JSON.parseObject(text, type);
    }

    public static <T> T parse(Object obj, TypeReference<T> type) {
        if (obj == null) {
            return null;
        }

        String json = toJsonString(obj);
        return parse(json, type);
    }

    /**
     * 从输入流当中读取JSON对象
     *
     * @param inputStream 输入流
     * @param cls         实际对象类型
     * @param <T>         泛型
     * @return 实际对象
     * @throws IOException 流异常
     */
    public static <T> T parse(InputStream inputStream, Class<T> cls) throws IOException {
        Reader       reader = new InputStreamReader(inputStream);
        int          ch;
        StringBuffer sb     = new StringBuffer();
        while ((ch = reader.read()) != -1) {
            sb.append((char) ch);
        }
        reader.close();
        return parse(sb.toString(), cls);
    }
}
