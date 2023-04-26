package com.fyp.engine.job.matcher;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class RuleConfig implements Serializable {
    private String  ruleName;
    private String  eventType;
    private Long    windowSize;
    private Integer threshold;
    private String  alertConfig;
}

