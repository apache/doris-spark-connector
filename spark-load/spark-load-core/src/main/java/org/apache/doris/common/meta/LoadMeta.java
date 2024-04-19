package org.apache.doris.common.meta;

import org.apache.doris.common.Constants;
import org.apache.doris.config.JobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig;

import lombok.Data;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class LoadMeta {

    private Long loadId;
    private Long txnId;
    private Long dbId;
    private Long signature;
    private Map<String, TableMeta> tableMeta;

    public EtlJobConfig getEtlJobConfig(JobConfig jobConfig) {
        Map<Long, EtlJobConfig.EtlTable> tables = new HashMap<>();
        getTableMeta().forEach((name, meta) -> {
            EtlJobConfig.EtlTable etlTable = new EtlJobConfig.EtlTable(meta.getIndexes().stream().map(
                    TableMeta.EtlIndex::toEtlIndex).collect(Collectors.toList()),
                    meta.getPartitionInfo().toEtlPartitionInfo());
            JobConfig.TaskInfo taskInfo = jobConfig.getLoadTasks().get(name);
            EtlJobConfig.EtlFileGroup fileGroup;
            Map<String, EtlJobConfig.EtlColumnMapping> columnMappingMap = taskInfo.toEtlColumnMappingMap();
            List<Long> partitionIds = meta.getPartitionInfo().partitions.stream()
                    .map(p -> p.partitionId).collect(Collectors.toList());
            switch (taskInfo.getType()) {
                case HIVE:
                    Map<String, String> properties = new HashMap<>(jobConfig.getHadoopProperties());
                    properties.put(Constants.HIVE_METASTORE_URIS, taskInfo.getHiveMetastoreUris());
                    fileGroup =
                            new EtlJobConfig.EtlFileGroup(EtlJobConfig.SourceType.HIVE, taskInfo.getHiveFullTableName(),
                                    properties, false, columnMappingMap, taskInfo.getWhere(),
                                    partitionIds);
                    break;
                case FILE:
                    List<String> columnList =
                            Arrays.stream(taskInfo.getColumns().split(",")).collect(Collectors.toList());
                    List<String> columnFromPathList = taskInfo.getColumnFromPath() == null ? Collections.emptyList() :
                            Arrays.stream(taskInfo.getColumnFromPath().split(",")).collect(Collectors.toList());
                    fileGroup =
                            new EtlJobConfig.EtlFileGroup(EtlJobConfig.SourceType.FILE, taskInfo.getPaths(), columnList,
                                    columnFromPathList, taskInfo.getFieldSep(), taskInfo.getLineDelim(), false,
                                    taskInfo.getFormat(), columnMappingMap, taskInfo.getWhere(), partitionIds);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported task type: " + taskInfo.getType());
            }
            etlTable.addFileGroup(fileGroup);
            tables.put(meta.getId(), etlTable);
        });
        String outputFilePattern = EtlJobConfig.getOutputFilePattern(jobConfig.getLabel(),
                EtlJobConfig.FilePatternVersion.V1);
        String label = jobConfig.getLabel();
        EtlJobConfig.EtlJobProperty properties = new EtlJobConfig.EtlJobProperty();
        EtlJobConfig etlJobConfig = new EtlJobConfig(tables, outputFilePattern, label, properties);
        etlJobConfig.outputPath =
                EtlJobConfig.getOutputPath(jobConfig.getSpark().getWorkingDir(), getDbId(), label,
                        getSignature());
        return etlJobConfig;
    }

}
