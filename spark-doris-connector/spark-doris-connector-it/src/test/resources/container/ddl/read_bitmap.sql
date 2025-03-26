DROP TABLE IF EXISTS tbl_read_tbl_bitmap;

create table tbl_read_tbl_bitmap (
datekey int,
hour int,
device_id bitmap BITMAP_UNION
)
aggregate key (datekey, hour)
distributed by hash(datekey, hour) buckets 1
properties(
  "replication_num" = "1"
);

insert into tbl_read_tbl_bitmap values
(20200622, 1, to_bitmap(243)),
(20200622, 2, bitmap_from_array([1,2,3,4,5,434543])),
(20200622, 3, to_bitmap(287667876573));