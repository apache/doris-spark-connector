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

package org.apache.doris.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Copied from Apache Doris org.apache.doris.sparkdpp.EtlJobConfig
 */
public class EtlJobConfig implements Serializable {
    // global dict
    public static final String GLOBAL_DICT_TABLE_NAME = "doris_global_dict_table_%d";
    public static final String DISTINCT_KEY_TABLE_NAME = "doris_distinct_key_table_%d_%s";
    public static final String DORIS_INTERMEDIATE_HIVE_TABLE_NAME = "doris_intermediate_hive_table_%d_%s";
    // tableId.partitionId.indexId.bucket.schemaHash
    public static final String TABLET_META_FORMAT = "%d.%d.%d.%d.%d";
    public static final String ETL_OUTPUT_FILE_FORMAT = "parquet";
    // dpp result
    public static final String DPP_RESULT_NAME = "dpp_result.json";
    // hdfsEtlPath/jobs/dbId/loadLabel/PendingTaskSignature
    private static final String ETL_OUTPUT_PATH_FORMAT = "%s/jobs/%d/%s/%d";
    private static final String ETL_OUTPUT_FILE_NAME_DESC_V1 =
            "version.label.tableId.partitionId.indexId.bucket.schemaHash.parquet";
    @JsonProperty(value = "tables")
    public Map<Long, EtlTable> tables;
    @JsonProperty(value = "outputPath")
    public String outputPath;
    @JsonProperty(value = "outputFilePattern")
    public String outputFilePattern;
    @JsonProperty(value = "label")
    public String label;
    @JsonProperty(value = "properties")
    public EtlJobProperty properties;
    @JsonProperty(value = "configVersion")
    public ConfigVersion configVersion;

    /**
     * for json deserialize
     */
    public EtlJobConfig() {
    }

    public EtlJobConfig(Map<Long, EtlTable> tables, String outputFilePattern, String label, EtlJobProperty properties) {
        this.tables = tables;
        // set outputPath when submit etl job
        this.outputPath = null;
        this.outputFilePattern = outputFilePattern;
        this.label = label;
        this.properties = properties;
        this.configVersion = ConfigVersion.V1;
    }

    public static String getOutputPath(String hdfsEtlPath, long dbId, String loadLabel, long taskSignature) {
        return String.format(ETL_OUTPUT_PATH_FORMAT, hdfsEtlPath, dbId, loadLabel, taskSignature);
    }

    public static String getOutputFilePattern(String loadLabel, FilePatternVersion filePatternVersion) {
        return String.format("%s.%s.%s.%s", filePatternVersion.name(), loadLabel, TABLET_META_FORMAT,
                ETL_OUTPUT_FILE_FORMAT);
    }

    public static String getDppResultFilePath(String outputPath) {
        return outputPath + "/" + DPP_RESULT_NAME;
    }

    public static String getTabletMetaStr(String filePath) throws Exception {
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String[] fileNameArr = fileName.split("\\.");
        // check file version
        switch (FilePatternVersion.valueOf(fileNameArr[0])) {
            case V1:
                // version.label.tableId.partitionId.indexId.bucket.schemaHash.parquet
                if (fileNameArr.length != ETL_OUTPUT_FILE_NAME_DESC_V1.split("\\.").length) {
                    throw new Exception(
                            "etl output file name error, format: " + ETL_OUTPUT_FILE_NAME_DESC_V1 + ", name: "
                                    + fileName);
                }
                long tableId = Long.parseLong(fileNameArr[2]);
                long partitionId = Long.parseLong(fileNameArr[3]);
                long indexId = Long.parseLong(fileNameArr[4]);
                int bucket = Integer.parseInt(fileNameArr[5]);
                int schemaHash = Integer.parseInt(fileNameArr[6]);
                // tableId.partitionId.indexId.bucket.schemaHash
                return String.format(TABLET_META_FORMAT, tableId, partitionId, indexId, bucket, schemaHash);
            default:
                throw new Exception("etl output file version error. version: " + fileNameArr[0]);
        }
    }

    public static EtlJobConfig configFromJson(String jsonConfig) throws JsonProcessingException {
        JsonMapper mapper = JsonMapper.builder().build();
        return mapper.readValue(jsonConfig, EtlJobConfig.class);
    }

    @Override
    public String toString() {
        return "EtlJobConfig{" + "tables=" + tables + ", outputPath='" + outputPath + '\'' + ", outputFilePattern='"
                + outputFilePattern + '\'' + ", label='" + label + '\'' + ", properties=" + properties + ", version="
                + configVersion + '}';
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String configToJson() throws JsonProcessingException {
        JsonMapper mapper = JsonMapper.builder().build();
        return mapper.writeValueAsString(this);
    }

    public enum ConfigVersion {
        V1
    }

    public enum FilePatternVersion {
        V1
    }

    public enum SourceType {
        FILE, HIVE
    }

    public static class EtlJobProperty implements Serializable {
        @JsonProperty(value = "strictMode")
        public boolean strictMode;
        @JsonProperty(value = "timezone")
        public String timezone;

        @Override
        public String toString() {
            return "EtlJobProperty{" + "strictMode=" + strictMode + ", timezone='" + timezone + '\'' + '}';
        }
    }

    public static class EtlTable implements Serializable {
        @JsonProperty(value = "indexes")
        public List<EtlIndex> indexes;
        @JsonProperty(value = "partitionInfo")
        public EtlPartitionInfo partitionInfo;
        @JsonProperty(value = "fileGroups")
        public List<EtlFileGroup> fileGroups;

        /**
         * for json deserialize
         */
        public EtlTable() {
        }

        public EtlTable(List<EtlIndex> etlIndexes, EtlPartitionInfo etlPartitionInfo) {
            this.indexes = etlIndexes;
            this.partitionInfo = etlPartitionInfo;
            this.fileGroups = Lists.newArrayList();
        }

        public void addFileGroup(EtlFileGroup etlFileGroup) {
            fileGroups.add(etlFileGroup);
        }

        @Override
        public String toString() {
            return "EtlTable{" + "indexes=" + indexes + ", partitionInfo=" + partitionInfo + ", fileGroups="
                    + fileGroups + '}';
        }
    }

    public static class EtlColumn implements Serializable {
        @JsonProperty(value = "columnName")
        public String columnName;
        @JsonProperty(value = "columnType")
        public String columnType;
        @JsonProperty(value = "isAllowNull")
        public boolean isAllowNull;
        @JsonProperty(value = "isKey")
        public boolean isKey;
        @JsonProperty(value = "aggregationType")
        public String aggregationType;
        @JsonProperty(value = "defaultValue")
        public String defaultValue;
        @JsonProperty(value = "stringLength")
        public int stringLength;
        @JsonProperty(value = "precision")
        public int precision;
        @JsonProperty(value = "scale")
        public int scale;
        @JsonProperty(value = "defineExpr")
        public String defineExpr;

        // for unit test
        public EtlColumn() {
        }

        public EtlColumn(String columnName, String columnType, boolean isAllowNull, boolean isKey,
                         String aggregationType, String defaultValue, int stringLength, int precision, int scale) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.isAllowNull = isAllowNull;
            this.isKey = isKey;
            this.aggregationType = aggregationType;
            this.defaultValue = defaultValue;
            this.stringLength = stringLength;
            this.precision = precision;
            this.scale = scale;
            this.defineExpr = null;
        }

        @Override
        public String toString() {
            return "EtlColumn{" + "columnName='" + columnName + '\'' + ", columnType='" + columnType + '\''
                    + ", isAllowNull=" + isAllowNull + ", isKey=" + isKey + ", aggregationType='" + aggregationType
                    + '\'' + ", defaultValue='" + defaultValue + '\'' + ", stringLength=" + stringLength
                    + ", precision=" + precision + ", scale=" + scale + ", defineExpr='" + defineExpr + '\'' + '}';
        }
    }

    public static class EtlIndexComparator implements Comparator<EtlIndex> {
        @Override
        public int compare(EtlIndex a, EtlIndex b) {
            int diff = a.columns.size() - b.columns.size();
            if (diff == 0) {
                return 0;
            } else if (diff > 0) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    public static class EtlIndex implements Serializable {
        @JsonProperty(value = "indexId")
        public long indexId;
        @JsonProperty(value = "columns")
        public List<EtlColumn> columns;
        @JsonProperty(value = "schemaHash")
        public int schemaHash;
        @JsonProperty(value = "indexType")
        public String indexType;
        @JsonProperty(value = "isBaseIndex")
        public boolean isBaseIndex;
        @JsonProperty(value = "schemaVersion")
        public int schemaVersion;

        /**
         * for json deserialize
         */
        public EtlIndex() {
        }

        public EtlIndex(long indexId, List<EtlColumn> etlColumns, int schemaHash, String indexType, boolean isBaseIndex,
                        int schemaVersion) {
            this.indexId = indexId;
            this.columns = etlColumns;
            this.schemaHash = schemaHash;
            this.indexType = indexType;
            this.isBaseIndex = isBaseIndex;
            this.schemaVersion = schemaVersion;
        }

        public EtlColumn getColumn(String name) {
            for (EtlColumn column : columns) {
                if (column.columnName.equals(name)) {
                    return column;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "EtlIndex{" + "indexId=" + indexId + ", columns=" + columns + ", schemaHash=" + schemaHash
                    + ", indexType='" + indexType + '\'' + ", isBaseIndex=" + isBaseIndex + ", schemaVersion="
                    + schemaVersion + '}';
        }
    }

    public static class EtlPartitionInfo implements Serializable {
        @JsonProperty(value = "partitionType")
        public String partitionType;
        @JsonProperty(value = "partitionColumnRefs")
        public List<String> partitionColumnRefs;
        @JsonProperty(value = "distributionColumnRefs")
        public List<String> distributionColumnRefs;
        @JsonProperty(value = "partitions")
        public List<EtlPartition> partitions;

        /**
         * for json deserialize
         */
        public EtlPartitionInfo() {
        }

        public EtlPartitionInfo(String partitionType, List<String> partitionColumnRefs,
                                List<String> distributionColumnRefs, List<EtlPartition> etlPartitions) {
            this.partitionType = partitionType;
            this.partitionColumnRefs = partitionColumnRefs;
            this.distributionColumnRefs = distributionColumnRefs;
            this.partitions = etlPartitions;
        }

        @Override
        public String toString() {
            return "EtlPartitionInfo{" + "partitionType='" + partitionType + '\'' + ", partitionColumnRefs="
                    + partitionColumnRefs + ", distributionColumnRefs=" + distributionColumnRefs + ", partitions="
                    + partitions + '}';
        }
    }

    public static class EtlPartition implements Serializable {
        @JsonProperty(value = "partitionId")
        public long partitionId;
        @JsonProperty(value = "startKeys")
        public List<Object> startKeys;
        @JsonProperty(value = "endKeys")
        public List<Object> endKeys;
        @JsonProperty(value = "isMaxPartition")
        public boolean isMaxPartition;
        @JsonProperty(value = "bucketNum")
        public int bucketNum;

        /**
         * for json deserialize
         */
        public EtlPartition() {
        }

        public EtlPartition(long partitionId, List<Object> startKeys, List<Object> endKeys, boolean isMaxPartition,
                            int bucketNum) {
            this.partitionId = partitionId;
            this.startKeys = startKeys;
            this.endKeys = endKeys;
            this.isMaxPartition = isMaxPartition;
            this.bucketNum = bucketNum;
        }

        @Override
        public String toString() {
            return "EtlPartition{" + "partitionId=" + partitionId + ", startKeys=" + startKeys + ", endKeys="
                    + endKeys + ", isMaxPartition=" + isMaxPartition + ", bucketNum=" + bucketNum + '}';
        }
    }

    public static class EtlFileGroup implements Serializable {
        @JsonProperty(value = "sourceType")
        public SourceType sourceType = SourceType.FILE;
        @JsonProperty(value = "filePaths")
        public List<String> filePaths;
        @JsonProperty(value = "fileFieldNames")
        public List<String> fileFieldNames;
        @JsonProperty(value = "columnsFromPath")
        public List<String> columnsFromPath;
        @JsonProperty(value = "columnSeparator")
        public String columnSeparator;
        @JsonProperty(value = "lineDelimiter")
        public String lineDelimiter;
        @JsonProperty(value = "isNegative")
        public boolean isNegative;
        @JsonProperty(value = "fileFormat")
        public String fileFormat;
        @JsonProperty(value = "columnMappings")
        public Map<String, EtlColumnMapping> columnMappings;
        @JsonProperty(value = "where")
        public String where;
        @JsonProperty(value = "partitions")
        public List<Long> partitions;
        @JsonProperty(value = "hiveDbTableName")
        public String hiveDbTableName;
        @JsonProperty(value = "hiveTableProperties")
        public Map<String, String> hiveTableProperties;

        // hive db table used in dpp, not serialized
        // set with hiveDbTableName (no bitmap column) or IntermediateHiveTable (created by global dict builder)
        // in spark etl job
        @JsonIgnore
        public String dppHiveDbTableName;

        public EtlFileGroup() {
        }

        // for data infile path
        public EtlFileGroup(SourceType sourceType, List<String> filePaths, List<String> fileFieldNames,
                            List<String> columnsFromPath, String columnSeparator, String lineDelimiter,
                            boolean isNegative, String fileFormat, Map<String, EtlColumnMapping> columnMappings,
                            String where, List<Long> partitions) {
            this.sourceType = sourceType;
            this.filePaths = filePaths;
            this.fileFieldNames = fileFieldNames;
            this.columnsFromPath = columnsFromPath;
            this.columnSeparator = Strings.isNullOrEmpty(columnSeparator) ? "\t" : columnSeparator;
            this.lineDelimiter = lineDelimiter;
            this.isNegative = isNegative;
            this.fileFormat = fileFormat;
            this.columnMappings = columnMappings;
            this.where = where;
            this.partitions = partitions;
        }

        // for data from table
        public EtlFileGroup(SourceType sourceType, String hiveDbTableName, Map<String, String> hiveTableProperties,
                            boolean isNegative, Map<String, EtlColumnMapping> columnMappings, String where,
                            List<Long> partitions) {
            this.sourceType = sourceType;
            this.hiveDbTableName = hiveDbTableName;
            this.hiveTableProperties = hiveTableProperties;
            this.isNegative = isNegative;
            this.columnMappings = columnMappings;
            this.where = where;
            this.partitions = partitions;
        }

        @Override
        public String toString() {
            return "EtlFileGroup{" + "sourceType=" + sourceType + ", filePaths=" + filePaths + ", fileFieldNames="
                    + fileFieldNames + ", columnsFromPath=" + columnsFromPath + ", columnSeparator='" + columnSeparator
                    + '\'' + ", lineDelimiter='" + lineDelimiter + '\'' + ", isNegative=" + isNegative
                    + ", fileFormat='" + fileFormat + '\'' + ", columnMappings=" + columnMappings + ", where='" + where
                    + '\'' + ", partitions=" + partitions + ", hiveDbTableName='" + hiveDbTableName + '\''
                    + ", hiveTableProperties=" + hiveTableProperties + '}';
        }
    }

    /**
     * FunctionCallExpr = functionName(args)
     * For compatibility with old designed functions used in Hadoop MapReduce etl
     * <p>
     * expr is more general, like k1 + 1, not just FunctionCall
     */
    public static class EtlColumnMapping implements Serializable {

        private static Map<String, String> functionMap =
                new ImmutableMap.Builder<String, String>().put("md5sum", "md5").build();

        @JsonProperty(value = "functionName")
        public String functionName;
        @JsonProperty(value = "args")
        public List<String> args;
        @JsonProperty(value = "expr")
        public String expr;

        public EtlColumnMapping() {
        }

        public EtlColumnMapping(String functionName, List<String> args) {
            this.functionName = functionName;
            this.args = args;
        }

        public EtlColumnMapping(String expr) {
            this.expr = expr;
        }

        public String toDescription() {
            StringBuilder sb = new StringBuilder();
            if (functionName == null) {
                sb.append(expr);
            } else {
                if (functionMap.containsKey(functionName)) {
                    sb.append(functionMap.get(functionName));
                } else {
                    sb.append(functionName);
                }
                sb.append("(");
                if (args != null) {
                    for (String arg : args) {
                        sb.append(arg);
                        sb.append(",");
                    }
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            return "EtlColumnMapping{" + "functionName='" + functionName + '\'' + ", args=" + args + ", expr=" + expr
                    + '}';
        }
    }

}
