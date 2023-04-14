package com.fyp.engine.common.exception;

import com.fyp.engine.common.enums.ReturnStatus;

/**
 * @Author lincn
 */
public class BusinessException extends BaseException {

    private static final long serialVersionUID = 1L;

    private Integer code;

    public BusinessException() {
    }

    public BusinessException(ReturnStatus status) {
        super(status.getDescription());
        this.code = status.getCode();
    }

    public BusinessException(ReturnStatus status, String msg) {
        super(msg);
        this.code = status.getCode();
    }


    public BusinessException(ReturnStatus status, String message, Throwable cause) {
        super(message, cause);
        this.code = status.getCode();
    }

    public BusinessException(Integer code, String msg) {
        super(msg);
        this.code = code;
    }

    public BusinessException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

}