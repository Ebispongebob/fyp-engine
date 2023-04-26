package com.fyp.engine.job.matcher;

import org.apache.flink.cep.pattern.Pattern;

public interface Handler<T, R extends T> {
    Pattern<T, R> doHandle();
}
