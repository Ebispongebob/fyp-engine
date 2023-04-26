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
import com.fyp.engine.job.matcher.MatchProcess;
import com.fyp.engine.job.matcher.MatchHandler;
import com.fyp.engine.job.matcher.RuleConfig;
import com.fyp.engine.job.message.EventMessage;
import com.fyp.engine.job.message.ReceivedMessage;
import com.fyp.engine.job.message.SinkMessage;
import com.fyp.engine.job.source.converter.EventMessageCompensator;
import com.fyp.engine.job.source.filter.EventMessagePreFilter;
import com.google.common.collect.Lists;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AdpRtRuleEngine extends BaseRtEngine<ReceivedMessage> implements Serializable {

    private static final String ruleEngineDriver              = "rule.engine.jdbc-driver";
    private static final String ruleEngineUrl                 = "rule.engine.jdbc-url";
    private static final String ruleEngineUsername            = "rule.engine.jdbc-username";
    private static final String ruleEnginePassword            = "rule.engine.jdbc-password";
    private static final String ruleEngineValidReferenceTable = "rule.engine.jdbc-valid-reference-table";
    private static final String ruleEngineEventTypeTable      = "rule.engine.jdbc-event-type-table";
    private static final String ruleTable                     = "rule.engine.jdbc-rule-table";
    private static final String ruleRelTable                  = "rule.engine.jdbc-rule-rel-table";

    public AdpRtRuleEngine(Configuration configuration) {
        super(configuration);
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            throw new BusinessException(ReturnStatus.SC_BAD_REQUEST, "args error");
        }
        new AdpRtRuleEngine(buildConfiguration(args[0], args[1])).run();
    }

    public static Configuration buildConfiguration(String referenceId, String topic) {
        Configuration configuration = new Configuration();
        configuration.setJobName("job_" + UUID.randomUUID());
        configuration.setCheckpointTimeout(10000);
        configuration.setRocksDb(true);
        configuration.setFsCheckpointStorage(false);
        configuration.setCheckpointInterval(10000);

        // source info
        SourceInfo sourceInfo = new SourceInfo();
        sourceInfo.setParallelism(3);
        sourceInfo.setWatermark(1000);
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setGroupId("job_consumer_group");
        consumerInfo.setTopic(topic);
        consumerInfo.setServers(config.getString("kafka.server"));
        sourceInfo.setConsumerInfo(consumerInfo);

        // match info
        MatchInfo matchInfo = new MatchInfo();
        matchInfo.setReferenceId(referenceId);

        // sink info
        SinkInfo sinkInfo = new SinkInfo();
        sinkInfo.setServers(config.getString("kafka.server"));
        sinkInfo.setTopic("adp_dispatcher");
        sinkInfo.setSinkType("kafka");

        configuration.setSourceInfo(sourceInfo);
        configuration.setMatchInfo(matchInfo);
        configuration.setSinkInfo(sinkInfo);

        return configuration;
    }

    @Override
    protected void initSink(StreamExecutionEnvironment env, SinkInfo sinkInfo, DataStream<Map<String, List<SinkMessage>>> filterStream) {
        filterStream.addSink(buildKafkaSink(sinkInfo))
                    .name("输出到kafka")
                    .uid(Uid.SINK_KAFKA);
        ;
    }

    /**
     * 构建sink
     */
    private FlinkKafkaProducer<Map<String, List<SinkMessage>>> buildKafkaSink(SinkInfo sinkInfo) {
        return KafkaFactory.buildSinkMessageSink(sinkInfo.getTopic(), sinkInfo.getServers());
    }

    @Override
    protected DataStream<Map<String, List<SinkMessage>>> initMatcher(StreamExecutionEnvironment env, MatchInfo matchInfo,
            DataStream<ReceivedMessage> sourceStream) {

        RuleConfig rule = buildRuleList(env, matchInfo.getReferenceId());

        // 2 sink message
        SingleOutputStreamOperator<SinkMessage> matchStream = sourceStream.map(receivedMessage -> {
            SinkMessage sinkMessage = new SinkMessage();
            sinkMessage.setId(receivedMessage.getId());
            sinkMessage.setUid(receivedMessage.getUid());
            sinkMessage.setEventType(receivedMessage.getEventType());
            sinkMessage.setReferenceId(receivedMessage.getReferenceId());
            sinkMessage.setEventTime(receivedMessage.getEventTime());
            sinkMessage.setReceivedTime(receivedMessage.getReceivedTime());
            sinkMessage.setRuleConfig(rule.getAlertConfig());
            return sinkMessage;
        });

        // init pattern
        Pattern<SinkMessage, SinkMessage> pattern = Pattern.begin("begin");

        // load pattern from db
        MatchHandler matchHandler = new MatchHandler(pattern, rule);
        Pattern<SinkMessage, SinkMessage> pattern1 = matchHandler.doHandle();

        // build pattern stream
        SingleOutputStreamOperator<Map<String, List<SinkMessage>>> process = CEP.pattern(matchStream, pattern1)
                                                                                // in process time
                                                                                .inProcessingTime()
                                                                                // collect
                                                                                .process(new MatchProcess())
                                                                                .uid(Uid.CEP)
                                                                                .name("match");
        process.print();
        return process;
    }

    /**
     * 根据ReferenceId获取Rule
     */
    private static RuleConfig buildRuleList(StreamExecutionEnvironment env, String referenceId) {
        EnvironmentSettings    settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // build sql
        String sql    = buildTableSql(ruleTable, SqlConstants.connectAdpRuleSql);
        String relSql = buildTableSql(ruleRelTable, SqlConstants.connectAdpRuleRelSql);

        // init table
        tableEnv.executeSql(sql);
        tableEnv.executeSql(relSql);

        // query
        TableResult tableResult = tableEnv.executeSql(SqlConstants.queryRuleReferRelByReferenceId.replaceAll("\\{referenceId}", referenceId));

        CloseableIterator<Row> rowData = tableResult.collect();

        RuleConfig rule = null;
        if (rowData.hasNext()) {
            Row next = rowData.next();
            rule = new RuleConfig();
            rule.setRuleName((String) next.getField("rule_name"));
            rule.setEventType((String) next.getField("event_type"));
            rule.setWindowSize(Long.valueOf((Integer) next.getField("window_size")));
            rule.setThreshold((Integer) next.getField("threshold"));
            rule.setAlertConfig((String) next.getField("alert_config"));
        }

        return rule;
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
                        buildQueryList(env, ruleEngineValidReferenceTable, SqlConstants.connectValidReferenceSql, SqlConstants.queryValidReferenceSql,
                                       FieldConstants.validReferenceId),
                        buildQueryList(env, ruleEngineEventTypeTable, SqlConstants.connectValidEventSql, SqlConstants.queryValidEventSql, FieldConstants.validEventType))
                )
                .name(FuncName.PRE_FILTER)
                // 事件补全
                .map(new EventMessageCompensator())
                .name(FuncName.COMPENSATE);

        return result;
    }

    /**
     * 构建source
     */
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

    /**
     * 使用CDC构建过滤列表
     */
    private static List<String> buildQueryList(StreamExecutionEnvironment env, String table, String connectSql, String querySql, String field) {
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

    /**
     * 通用构建CDC Table
     */
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
