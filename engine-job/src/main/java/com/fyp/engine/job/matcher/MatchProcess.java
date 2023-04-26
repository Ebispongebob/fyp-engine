package com.fyp.engine.job.matcher;

import com.fyp.engine.job.message.ReceivedMessage;
import com.fyp.engine.job.message.SinkMessage;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MatchProcess extends PatternProcessFunction<SinkMessage, Map<String, List<SinkMessage>>> {

    @Override
    public void processMatch(Map<String, List<SinkMessage>> match, Context ctx, Collector<Map<String, List<SinkMessage>>> out) throws Exception {
        out.collect(match);
    }
}
