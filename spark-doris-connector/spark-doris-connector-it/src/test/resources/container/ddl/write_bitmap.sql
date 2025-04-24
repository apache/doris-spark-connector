DROP TABLE IF EXISTS tbl_write_tbl_bitmap;

create table tbl_write_tbl_bitmap (
datekey int,
hour int,
device_id bitmap BITMAP_UNION
)
aggregate key (datekey, hour)
distributed by hash(datekey, hour) buckets 1
properties(
  "replication_num" = "1"
);