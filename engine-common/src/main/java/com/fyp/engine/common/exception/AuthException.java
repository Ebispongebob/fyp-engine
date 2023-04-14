package com.fyp.engine.common.exception;

/**
 * @author MuxBwf
 * @Description: 认证异常
 */
public class AuthException extends BaseException {
    /**
     * uid
     */
    private static final long serialVersionUID = 7805981291350841911L;

    /**
     * 默认构造函数
     */
    public AuthException() {
        super("Business exception.");
    }

    /**
     * 构造函数
     * @param errMsg 异常消息
     */
    public AuthException(String errMsg) {
        super(errMsg);
    }

    /**
     * 业务异常
     * @param cause 原始异常
     */
    public AuthException(Throwable cause) {
        super(cause);
    }
}
