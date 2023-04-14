package com.fyp.engine.common.enums;

/**
 * 统一返回码定义
 * @Author ruitao.wei.
 */
public enum ReturnStatus {

    //成功
    SUCCESS(0, "成功"),
    SC_BAD_REQUEST(400, "错误的入参(参数基础验证失败)"),
    SC_UNAUTHORIZED(401, "没有权限"),
    SC_FORBIDDEN(403, "拒绝操作"),
    SC_NOT_FOUND(404, "找不到资源"),
    //系统内部异常
    SC_INTERNAL_SERVER_ERROR(500, "系统内部异常"),

    //未登录
    NO_LOGIN(600, "未登录"),

    //逻辑验证未通过
    LOGICAL_VALIDATE_FAILED(601, "逻辑验证未通过"),

    //消息重复发送
    MESSAGE_REPEAT(602, "消息重复发送"),

    //中断异常
    INTERRUPT(603, "中断异常"),

    //内部执行时间过长
    EXECUTION_TIME_TOO_LONG(604, "内部执行时间过长"),

    // 执行失败且无法重试
    EXECUTION_FAIL_WITHOUT_RETRY(605, "执行失败且无法重试"),

    // job提交失败
    JOB_SUBMIT_FAILED(606, "job提交失败"),

    // 创建和启动新driver失败
    DRIVER_CREATE_AND_LAUNCH_FAILED(607, "创建和启动新driver失败"),

    // 状态校验错误
    STATUS_CHECK_ERROR(608, "状态校验错误"),

    /**
     * 错误提示，当前用于判断是否显示堆栈信息
     */
    TIPS(609, "提示"),

    // 记录已存在错误
    RECORD_EXISTED_ERROR(610, "记录已存在"),

    SQL_FORMAT_ERROR(611, "sql格式异常");

    private Integer code;

    private String description;

    ReturnStatus(Integer code, String description) {
        this.code = code;
        this.description = description;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
