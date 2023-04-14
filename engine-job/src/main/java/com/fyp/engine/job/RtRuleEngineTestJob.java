package com.fyp.engine.job;

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
import com.google.common.collect.Lists;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
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
import java.util.Objects;

public class RtRuleEngineTestJob extends BaseRtEngine<ReceivedMessage> {

    private static final String ruleEngineDriver              = "rule.engine.jdbc-driver";
    private static final String ruleEngineUrl                 = "rule.engine.jdbc-url";
    private static final String ruleEngineUsername            = "rule.engine.jdbc-username";
    private static final String ruleEnginePassword            = "rule.engine.jdbc-password";
    private static final String ruleEngineValidReferenceTable = "rule.engine.jdbc-valid-reference-table";
    private static final String ruleEngineEventTypeTable      = "rule.engine.jdbc-event-type-table";

    public RtRuleEngineTestJob(Configuration configuration) {
        super(configuration);
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setJobName("test_job");
        configuration.setCheckpointTimeout(10000);
        configuration.setRocksDb(true);
        configuration.setFsCheckpointStorage(false);
        configuration.setCheckpointInterval(10000);

        // source info
        SourceInfo sourceInfo = new SourceInfo();
        sourceInfo.setParallelism(3);
        sourceInfo.setWatermark(1000);
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setGroupId("test_group");
        consumerInfo.setTopic("source_topic_test");
        consumerInfo.setServers("localhost:9092");
        sourceInfo.setConsumerInfo(consumerInfo);

        // sink info
        SinkInfo sinkInfo = new SinkInfo();
        sinkInfo.setServers("localhost:9092");
        sinkInfo.setTopic("sink_topic_test");
        sinkInfo.setSinkType("kafka");

        // match info
        MatchInfo matchInfo = new MatchInfo();

        configuration.setSourceInfo(sourceInfo);
        configuration.setMatchInfo(matchInfo);
        configuration.setSinkInfo(sinkInfo);

        new RtRuleEngineTestJob(configuration).run();
    }

    @Override
    protected void initSink(StreamExecutionEnvironment env, SinkInfo sinkInfo, DataStream<ReceivedMessage> filterStream) {
        filterStream.sinkTo(buildKafkaSink(sinkInfo));
    }

    @Override
    protected DataStream<ReceivedMessage> initMatcher(StreamExecutionEnvironment env, MatchInfo matchInfo,
            DataStream<ReceivedMessage> sourceStream) {
        Pattern<ReceivedMessage, ?> pattern = Pattern.<ReceivedMessage>begin("pre_check_time")
                                                     .where(new IterativeCondition<ReceivedMessage>() {
                                                         @Override
                                                         public boolean filter(ReceivedMessage value, Context<ReceivedMessage> ctx) {
                                                             // 接收时间应大于事件时间
                                                             return value.getReceivedTime() - value.getEventTime() > 0;
                                                         }
                                                     })
                                                     .next("match_alert_rule")
                                                     .where(new IterativeCondition<ReceivedMessage>() {
                                                         @Override
                                                         public boolean filter(ReceivedMessage value, Context<ReceivedMessage> ctx) {
                                                             // 接收时间应大于事件时间
                                                             return Objects.equals(value.getReferenceId(), "kafka-app1");
                                                         }
                                                     });

        PatternStream<ReceivedMessage> patternStream = CEP.pattern(sourceStream, pattern)
                                                          .inProcessingTime();

        SingleOutputStreamOperator<String> select = patternStream.select(pattern1 -> "event-ex1: 触发");

        select.print();
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
                        buildRuleList(env, ruleEngineValidReferenceTable, SqlConstants.connectValidReferenceSql, SqlConstants.queryValidReferenceSql, FieldConstants.validReferenceId),
                        buildRuleList(env, ruleEngineEventTypeTable, SqlConstants.connectValidEventSql, SqlConstants.queryValidEventSql, FieldConstants.validEventType))
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

    private static List<String> buildRuleList(StreamExecutionEnvironment env, String table, String connectSql, String querySql, String field) {
        EnvironmentSettings    settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // build sql
        String sql = buildTableSql(table, connectSql);

        // init table
        tableEnv.executeSql(sql);

        // query
        TableResult tableResult = tableEnv.executeSql(querySql);

        CloseableIterator<Row> rowData = tableResult.collect();
        List<String>           result  = Lists.newArrayList();
        while (rowData.hasNext()) {
            Row    next   = rowData.next();
            String _field = (String) next.getField(field);
            result.add(_field);
        }

        return result;

    }

    private static String buildTableSql(String table, String connectSql) {
        return connectSql
                .replaceAll(SqlConstants.object, config.getString(table))
                .replaceAll(SqlConstants.url, config.getString(ruleEngineUrl))
                .replaceAll(SqlConstants.driver, config.getString(ruleEngineDriver))
                .replaceAll(SqlConstants.username, config.getString(ruleEngineUsername))
                .replaceAll(SqlConstants.password, config.getString(ruleEnginePassword))
                .replaceAll(SqlConstants.table, config.getString(table));
    }
}
