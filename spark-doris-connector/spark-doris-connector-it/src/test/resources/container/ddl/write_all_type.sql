DROP TABLE IF EXISTS tbl_write_tbl_all_types;

CREATE TABLE tbl_write_tbl_all_types (
`id` int,
`c1` boolean,
`c2` tinyint,
`c3` smallint,
`c4` int,
`c5` bigint,
`c6` largeint,
`c7` float,
`c8` double,
`c9` decimal(12,4),
`c10` date,
`c11` datetime,
`c12` char(1),
`c13` varchar(256),
`c14` string,
`c15` Array<String>,
`c16` Map<String, String>,
`c17` Struct<name: String, age: int>,
`c18` JSON,
`c19` VARIANT
)
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 2
PROPERTIES (
"replication_num" = "1",
"light_schema_change" = "true"
);

