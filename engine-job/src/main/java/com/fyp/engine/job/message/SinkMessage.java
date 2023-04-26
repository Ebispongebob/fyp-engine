package com.fyp.engine.job.message;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SinkMessage extends ReceivedMessage {
    private String ruleConfig;
}
