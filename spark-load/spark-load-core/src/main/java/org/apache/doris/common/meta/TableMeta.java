// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.meta;

import org.apache.doris.sparkdpp.EtlJobConfig;

import lombok.Data;

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
