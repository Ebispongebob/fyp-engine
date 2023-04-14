package com.fyp.engine.job.source.converter;

import com.fyp.engine.common.utils.UUIDUtil;
import com.fyp.engine.job.message.EventMessage;
import com.fyp.engine.job.message.ReceivedMessage;
import org.apache.flink.api.common.functions.MapFunction;

public class EventMessageCompensator implements MapFunction<EventMessage, ReceivedMessage> {
    @Override
    public ReceivedMessage map(EventMessage eventMessage) throws Exception {
        ReceivedMessage receivedMessage = new ReceivedMessage();
        receivedMessage.setId(eventMessage.getId());
        receivedMessage.setUid(UUIDUtil.randomUUID());
        receivedMessage.setReferenceId(eventMessage.getReferenceId());
        receivedMessage.setEventTime(eventMessage.getEventTime());
        receivedMessage.setEventType(eventMessage.getEventType());
        receivedMessage.setReceivedTime(System.currentTimeMillis());
        return receivedMessage;
    }
}
