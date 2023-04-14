package com.fyp.engine.common.utils;

import java.io.Serializable;
import java.util.UUID;

public class UUIDUtil implements Serializable {

    /**
     * 生成随机的UUID字符串
     * @return UUID字符串
     */
    public static String randomUUID(){
        String id = UUID.randomUUID().toString().replaceAll("-","");
        return id;
    }
}
