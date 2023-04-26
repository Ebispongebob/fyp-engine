package com.fyp.engine.job.matcher;

import com.fyp.engine.common.exception.EngineException;
import com.fyp.engine.job.message.SinkMessage;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Objects;

public class MatchHandler implements Handler<SinkMessage, SinkMessage> {

    private Pattern<SinkMessage, SinkMessage> pattern;

    private RuleConfig rule;

    public MatchHandler(Pattern<SinkMessage, SinkMessage> pattern, RuleConfig rule) {
        this.pattern = pattern;
        this.rule = rule;
    }

    public Pattern<SinkMessage, SinkMessage> doHandle() {
        // 判断事件类型
        if (Objects.nonNull(rule.getEventType())) {
            where(rule.getEventType());
        }
        // 判断窗口大小
        if (rule.getWindowSize().equals(-1L)) {
            within(rule.getWindowSize());
        }
        //判断阈值
        if (rule.getThreshold() != -1) {
            threshold(rule.getThreshold());
        }
        return this.pattern;
    }

    public void where(String eventType) {
        matchAction(pattern, ActionEnum.WHERE, eventType, null, null);
    }

    public void within(Long timeWindow) {
        matchAction(pattern, ActionEnum.WITHIN, null, timeWindow, null);
    }

    public void threshold(Integer times) {
        matchAction(pattern, ActionEnum.TIMES, null, null, times);
    }

    private static void matchAction(Pattern<SinkMessage, SinkMessage> pattern, ActionEnum action, String eventType,
            Long timeWindow, Integer threshold) {
        switch (action) {
        case WHERE:
            pattern.where(new IterativeCondition<SinkMessage>() {
                @Override
                public boolean filter(SinkMessage value, Context<SinkMessage> ctx) {
                    return value.getEventType().equals(eventType);
                }
            });
            break;
        case WITHIN:
            pattern.within(Time.seconds(timeWindow));
            break;
        case TIMES:
            pattern.times(threshold);
            break;
        default:
            throw new EngineException("error action");
        }
    }
}
