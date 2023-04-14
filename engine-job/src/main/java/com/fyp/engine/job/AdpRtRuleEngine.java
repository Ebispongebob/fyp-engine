package com.fyp.engine.job;

import com.fyp.engine.common.enums.ReturnStatus;
import com.fyp.engine.common.exception.BusinessException;
import com.fyp.engine.common.utils.OperatorUtils;
import com.fyp.engine.job.api.KafkaFactory;
import com.fyp.engine.job.config.Configuration;
import com.fyp.engine.job.constants.FieldConstants;
import com.fyp.engine.job.constants.FuncName;
import com.fyp.engine.job.constants.SqlConstants;
import com.fyp.engine.job.constants.Uid;
import com.fyp.engine.job.info.ConsumerInfo;
import com.fyp.engine.job.info.MatchInfo;
import com.fyp.engine.job.info.SinkInfo;
import com.fyp.engine.job.info.SourceInfo;
import com.fyp.engine.job.message.EventMessage;
import com.fyp.engine.job.message.ReceivedMessage;
import com.fyp.engine.job.source.converter.EventMessageCompensator;
import com.fyp.engine.job.source.filter.EventMessagePreFilter;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.List;

public class AdpRtRuleEngine extends BaseRtEngine<ReceivedMessage> {

    private static final String ruleEngineDriver              = "rule.engine.jdbc-driver";
    private static final String ruleEngineUrl                 = "rule.engine.jdbc-url";
    private static final String ruleEngineUsername            = "rule.engine.jdbc-username";
    private static final String ruleEnginePassword            = "rule.engine.jdbc-password";
    private static final String ruleEngineValidReferenceTable = "rule.engine.jdbc-valid-reference-table";
    private static final String ruleEngineEventTypeTable      = "rule.engine.jdbc-event-type-table";
    private static final String ruleEngineTable               = "rule.engine.jdbc-rule-table";

    public AdpRtRuleEngine(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected void initSink(StreamExecutionEnvironment env, SinkInfo sinkInfo, DataStream<ReceivedMessage> filterStream) {
        Sink sink = null;
        switch (sinkInfo.getSinkType()) {
        case "kafka":
            sink = buildKafkaSink(sinkInfo);
            break;
        default:
            throw new BusinessException(ReturnStatus.SC_BAD_REQUEST, "sink type not found");
        }
        filterStream.sinkTo(sink);
    }

    @Override
    protected DataStream<ReceivedMessage> initMatcher(StreamExecutionEnvironment env, MatchInfo matchInfo,
            DataStream<ReceivedMessage> sourceStream) {

        return null;
    }

    @Override
    protected DataStream<ReceivedMessage> initSource(StreamExecutionEnvironment env, SourceInfo sourceInfo) {
        // 初始化主数据源
        SourceFunction<EventMessage> sourceFunction;

        // 构建 kafka source
        sourceFunction = buildKafkaSource(sourceInfo.getConsumerInfo());
        DataStreamSource<EventMessage> dataStreamSource = env.addSource(sourceFunction);

        // 设置source并行度
        int pall = OperatorUtils.calParallelism(env, sourceInfo.getParallelism());
        dataStreamSource
                .name(String.format("%s[%s]", "Kafka", sourceInfo.getConsumerInfo().getTopic()))
                .setParallelism(pall)
                .uid(Uid.SOURCE);

        // 预处理
        SingleOutputStreamOperator<ReceivedMessage> result = dataStreamSource
                // 事件前置过滤
                .filter(new EventMessagePreFilter(
                        buildRuleList(env, ruleEngineValidReferenceTable),
                        buildRuleList(env, ruleEngineEventTypeTable))
                )
                .name(FuncName.PRE_FILTER)
                // 事件补全
                .map(new EventMessageCompensator())
                .name(FuncName.COMPENSATE);

        return result;
    }

    private FlinkKafkaConsumer<EventMessage> buildKafkaSource(ConsumerInfo consumerInfo) {

        FlinkKafkaConsumer<EventMessage> consumer =
                KafkaFactory.buildEventMsgSource(consumerInfo.getTopic(), consumerInfo.getServers(), consumerInfo.getGroupId());

        // 指定时间或者最新时间
        long start = consumerInfo.getStartTime();
        if (start > 0) {
            consumer.setStartFromTimestamp(start);
        }

        return consumer;
    }

    private KafkaSink<ReceivedMessage> buildKafkaSink(SinkInfo sinkInfo) {

        KafkaSink<ReceivedMessage> sink =
                KafkaFactory.buildReceivedMsgSink(sinkInfo.getTopic(), sinkInfo.getServers());

        return sink;
    }

    private static List<String> buildRuleList(StreamExecutionEnvironment env, String table) {
        EnvironmentSettings    settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // build sql
        String sql = buildTableSql(table, (ParameterTool) env.getConfig().getGlobalJobParameters());

        // init table
        tableEnv.executeSql(sql);

        // query
        TableResult            tableResult = tableEnv.executeSql(SqlConstants.queryValidReferenceSql);
        CloseableIterator<Row> rowData     = tableResult.collect();
        List<String>           result      = Lists.newArrayList();
        while (rowData.hasNext()) {
            Row    next        = rowData.next();
            String referenceId = (String) next.getField(FieldConstants.validReferenceId);
            result.add(referenceId);
        }

        return result;

    }

    private static String buildTableSql(String table, ParameterTool param) {
        return SqlConstants.connectValidReferenceSql
                .replaceAll(SqlConstants.url, param.get(ruleEngineUrl))
                .replaceAll(SqlConstants.driver, param.get(ruleEngineDriver))
                .replaceAll(SqlConstants.username, param.get(ruleEngineUsername))
                .replaceAll(SqlConstants.password, param.get(ruleEnginePassword))
                .replaceAll(SqlConstants.table, param.get(table));
    }
}
