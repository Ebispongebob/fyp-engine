package com.fyp.engine.job.source.filter;

import com.fyp.engine.job.message.EventMessage;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.List;

public class EventMessagePreFilter implements FilterFunction<EventMessage> {

    private List<String> validReferenceIds;
    private List<String> validEvenTypes;

    public EventMessagePreFilter() {
    }

    public EventMessagePreFilter(List<String> validReferenceIds, List<String> validEvenTypes) {
        this.validReferenceIds = validReferenceIds;
        this.validEvenTypes = validEvenTypes;
    }

    @Override
    public boolean filter(EventMessage eventMessage) throws Exception {
        // 过滤未配置或不存在的referenceId
        if (!getValidReferenceId().contains(eventMessage.getReferenceId())) {
            return false;
        }
        // 过滤未配置或不存在的事件类型
        if (!getValidEventType().contains(eventMessage.getEventType())) {
            return false;
        }

        return true;
    }

    private List<String> getValidReferenceId() {
        return validReferenceIds;
    }

    private List<String> getValidEventType() {
        return validEvenTypes;
    }
}
