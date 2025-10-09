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
                                      __source_ts_ms bigint NULL,
                                      __op varchar(10) NULL,
                                      __table varchar(100) NULL,
                                      __db varchar(50) NULL,
                                      __deleted varchar(10) NULL,
                                      __dt datetime NULL
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