package com.fyp.engine.job.message;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class ReceivedMessage extends EventMessage{
    private String uid;
    private Long receivedTime;
}
