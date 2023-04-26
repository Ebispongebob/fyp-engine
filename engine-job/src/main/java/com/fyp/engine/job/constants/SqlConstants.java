package com.fyp.engine.job.constants;

public interface SqlConstants {

    String object   = "\\{object}";
    String url      = "\\{url}";
    String table    = "\\{table}";
    String username = "\\{username}";
    String password = "\\{password}";
    String driver   = "\\{driver}";

    String connectValidReferenceSql = "CREATE TABLE valid_reference (" +
            "  id BIGINT," +
            "  reference_id STRING," +
            "  enable BOOLEAN" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = '{url}'," +
            "  'table-name' = '{table}'," +
            "  'username' = '{username}'," +
            "  'password' = '{password}'," +
            "  'driver' = '{driver}'" +
            ")";

    String connectValidEventSql = "CREATE TABLE event_type (" +
            "  id BIGINT," +
            "  event_type STRING," +
            "  enable BOOLEAN" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = '{url}'," +
            "  'table-name' = '{table}'," +
            "  'username' = '{username}'," +
            "  'password' = '{password}'," +
            "  'driver' = '{driver}'" +
            ")";

    String connectAdpRuleSql = "CREATE TABLE adp_rule (" +
            "  id INT," +
            "  rule_name STRING," +
            "  event_type STRING," +
            "  window_size INT," +
            "  threshold INT," +
            "  alert_config STRING," +
            "  create_time TIMESTAMP" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = '{url}'," +
            "  'table-name' = '{table}'," +
            "  'username' = '{username}'," +
            "  'password' = '{password}'," +
            "  'driver' = '{driver}'" +
            ")";
    String connectAdpRuleRelSql = "CREATE TABLE rule_reference_rel (" +
            "  reference_id STRING," +
            "  rule_name STRING" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = '{url}'," +
            "  'table-name' = '{table}'," +
            "  'username' = '{username}'," +
            "  'password' = '{password}'," +
            "  'driver' = '{driver}'" +
            ")";


    String queryValidReferenceSql         = "SELECT * FROM valid_reference WHERE enable is true";
    String queryValidEventSql             = "select id, event_type, enable from event_type where enable is true";
    String queryRuleReferRelByReferenceId = "SELECT adp_rule.rule_name,adp_rule.event_type,adp_rule.window_size,adp_rule.threshold,adp_rule.alert_config FROM adp_rule JOIN rule_reference_rel ON adp_rule.rule_name = rule_reference_rel.rule_name WHERE rule_reference_rel.reference_id = '{referenceId}'";
}
