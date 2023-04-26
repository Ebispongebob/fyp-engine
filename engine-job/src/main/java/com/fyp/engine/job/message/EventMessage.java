package com.fyp.engine.job.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventMessage implements Serializable {

    private Long   id;
    private String eventType;
    private String referenceId;
    private Long   eventTime;
}