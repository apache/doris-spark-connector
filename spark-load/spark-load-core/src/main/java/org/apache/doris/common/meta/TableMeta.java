package org.apache.doris.common.meta;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

import org.apache.doris.sparkdpp.EtlJobConfig;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class TableMeta {

    private Long id;
    private List<EtlIndex> indexes;
    private EtlPartitionInfo partitionInfo;

    public static class EtlIndex implements Serializable {
        public long indexId;
        public List<EtlJobConfig.EtlColumn> columns;
        public int schemaHash;
        public String indexType;
        public boolean isBaseIndex;

        public EtlIndex() {

        }

        public EtlJobConfig.EtlIndex toEtlIndex() {
            return new EtlJobConfig.EtlIndex(indexId, columns, schemaHash, indexType, isBaseIndex);
        }

    }

    public static class EtlPartitionInfo implements Serializable {
        public String partitionType;
        public List<String> partitionColumnRefs;
        public List<String> distributionColumnRefs;
        public List<EtlPartition> partitions;

        public EtlPartitionInfo() {
        }

        public EtlJobConfig.EtlPartitionInfo toEtlPartitionInfo() {
            return new EtlJobConfig.EtlPartitionInfo(partitionType, partitionColumnRefs, distributionColumnRefs,
                    partitions.stream().map(EtlPartition::toEtlPartition).collect(Collectors.toList()));
        }

    }

    public static class EtlPartition implements Serializable {
        public long partitionId;
        public List<Object> startKeys;
        public List<Object> endKeys;
        public boolean isMaxPartition;
        public int bucketNum;

        public EtlPartition() {
        }

        public EtlJobConfig.EtlPartition toEtlPartition() {
            return new EtlJobConfig.EtlPartition(partitionId, startKeys, endKeys, isMaxPartition, bucketNum);
        }
    }

}
