package com.fyp.engine.common.exception;

/**
 * @author MuxBwf
 */
public abstract class BaseException extends RuntimeException {
    /**
     * uid
     */
    private static final long serialVersionUID = 8037891447646609768L;

    /**
     * 默认构造函数
     */
    public BaseException() {
    }

    /**
     * 构造函数
     * @param errMsg 异常消息
     */
    public BaseException(String errMsg) {
        super(errMsg);
    }

    /**
     * 构造函数
     * @param cause 原始异常
     */
    public BaseException(Throwable cause) {
        super(cause);
    }

    /**
     * 构造函数
     * @param errMsg 异常消息
     * @param cause 原始异常
     */
    public BaseException(String errMsg, Throwable cause) {
        super(errMsg, cause);
    }

}
