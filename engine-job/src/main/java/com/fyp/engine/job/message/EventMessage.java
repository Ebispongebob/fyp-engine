package com.fyp.engine.job.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventMessage {

    private Long   id;
    private String eventType;
    private String referenceId;
    private Long   eventTime;
}