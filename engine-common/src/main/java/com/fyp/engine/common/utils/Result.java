package com.fyp.engine.common.utils;

import java.io.Serializable;

/**
 * @author MuxBwf
 */
public class Result implements Serializable {

    public static final int SUCCESS = 0;

    public static final int FAILURE = -1;

    public static final int AUTH = -2;

    /**
     * 状态
     */
    private int status;

    /**
     * 消息
     */
    private String msg;

    /**
     * 响应数据
     */
    private Object re;

    public static Result fail(int status, String msg) {
        Result result = new Result();
        result.setMsg(msg);
        result.setStatus(status);
        return result;
    }

    public static Result fail(String msg) {
        Result result = new Result();
        result.setMsg(msg);
        result.setStatus(FAILURE);
        return result;
    }

    public static Result success(Object re) {
        Result result = new Result();
        result.setStatus(SUCCESS);
        result.setRe(re);
        return result;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Object getRe() {
        return re;
    }

    public void setRe(Object re) {
        this.re = re;
    }

    @Override
    public String toString() {
        return "Result{" +
                "status=" + status +
                ", msg='" + msg + '\'' +
                ", re=" + re +
                '}';
    }
}
