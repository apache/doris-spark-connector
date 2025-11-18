SET enable_unicode_name_support = true;
DROP TABLE IF EXISTS `tbl_unicode_col`;
CREATE TABLE `tbl_unicode_col` (
  `序号` int NULL,
  `内容` text NULL
) ENGINE=OLAP
DUPLICATE KEY(`序号`)
DISTRIBUTED BY HASH(`序号`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);