package com.fyp.engine.job.constants;

public interface FuncName {

    String PARTICIPATION_LIMIT = "参与次数限制";

    String LATE_DATA = "未匹配数据记录";

    String COMPENSATE = "数据无效修正";

    String PRE_FILTER = "事件前置过滤";

    String ID_TAIL_FILTER = "用户id尾号过滤";

    String USER_GROUP_SOURCE = "分群数据源";

    String USER_GROUP_FILTER = "分群过滤";

    String TIME_OUT = "生成超时系统事件";

    String SORT = "按eventTime排序";

    String DELAY_FILTER = "延迟事件过滤";
}
