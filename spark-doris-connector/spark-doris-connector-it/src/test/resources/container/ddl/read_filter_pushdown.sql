DROP TABLE IF EXISTS tbl_expression_notpushdown;
CREATE TABLE tbl_expression_notpushdown (
                                      ID decimal(38,10) NULL,
                                      NAME varchar(300) NULL,
                                      AGE decimal(38,10) NULL,
                                      CREATE_TIME datetime(3) NULL,
                                      A1 varchar(300) NULL,
                                      A2 varchar(300) NULL,
                                      A3 varchar(300) NULL,
                                      A4 varchar(300) NULL,
                                      __source_ts_ms bigint NULL COMMENT "数据采入kafka的时间戳（集成任务自建）",
                                      __op varchar(10) NULL COMMENT "操作类型 c:增加 u:更新 d:删除（集成任务自建）r:稽核后补采",
                                      __table varchar(100) NULL COMMENT "源表表名（集成任务自建）",
                                      __db varchar(50) NULL COMMENT "源库名称（集成任务自建）",
                                      __deleted varchar(10) NULL COMMENT "删除标志：false代表有效，true代表已删除（集成任务自建）",
                                      __dt datetime NULL COMMENT "入库时间，分区字段（集成任务自建）"
) ENGINE=OLAP
DUPLICATE KEY(ID)
DISTRIBUTED BY HASH(`ID`) BUCKETS 2
PROPERTIES (
"replication_num" = "1",
"light_schema_change" = "true"
);

insert into tbl_expression_notpushdown values(1, 'name1', 18, '2021-01-01 00:00:00.000', 'a1', 'a2', 'a3', 'a4', 1609459200000, 'c', 'tbl_read_tbl_all_types', 'db_read_tbl_all_types', false, '2021-01-01 00:00:00');
insert into tbl_expression_notpushdown values(2, 'name2', 19, '2021-01-01 00:00:00.000', 'a1', 'a2', 'a3', 'a4', 1609459200000, 'c', 'tbl_read_tbl_all_types', 'db_read_tbl_all_types', false, '2021-01-01 00:00:00');
insert into tbl_expression_notpushdown values(3, 'name3', 20, '2021-01-01 00:00:00.000', 'a1', 'a2', 'a3', 'a4', 1609459200000, 'c', 'tbl_read_tbl_all_types', 'db_read_tbl_all_types', false, '2021-01-01 00:00:00');
insert into tbl_expression_notpushdown values(4, 'name4', 21, '2021-01-01 00:00:00.000', 'a1', 'a2', 'a3', 'a4', 1609459200000, 'c', 'tbl_read_tbl_all_types', 'db_read_tbl_all_types', false, '2021-01-01 00:00:00');
insert into tbl_expression_notpushdown values(5, 'name5', 22, '2021-01-01 00:00:00.000', 'a1', 'a2', 'a3', 'a4', 1609459200000, 'c', 'tbl_read_tbl_all_types', 'db_read_tbl_all_types', false, '2021-01-01 00:00:00');