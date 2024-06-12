package org.apache.doris.common.meta;

import org.apache.doris.common.Constants;
import org.apache.doris.exception.SparkLoadException;
import org.apache.doris.config.JobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Data
public class LoadMeta {

    private Long loadId;
    private Long txnId;
    private Long dbId;
    private Long signature;
    private Map<String, TableMeta> tableMeta;

    public EtlJobConfig getEtlJobConfig(JobConfig jobConfig) throws SparkLoadException {
        Map<Long, EtlJobConfig.EtlTable> tables = new HashMap<>();
        for (Map.Entry<String, TableMeta> entry : getTableMeta().entrySet()) {
            String name = entry.getKey();
            TableMeta meta = entry.getValue();
            EtlJobConfig.EtlTable etlTable = new EtlJobConfig.EtlTable(meta.getIndexes().stream().map(
                    TableMeta.EtlIndex::toEtlIndex).collect(Collectors.toList()),
                    meta.getPartitionInfo().toEtlPartitionInfo());
            JobConfig.TaskInfo taskInfo = jobConfig.getLoadTasks().get(name);
            EtlJobConfig.EtlFileGroup fileGroup;
            Map<String, EtlJobConfig.EtlColumnMapping> columnMappingMap = taskInfo.toEtlColumnMappingMap();
            checkMapping(etlTable, columnMappingMap);
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
                    List<String> columnList = Collections.emptyList();
                    if (StringUtils.isNoneBlank(taskInfo.getColumns())) {
                        columnList = Arrays.stream(taskInfo.getColumns().split(",")).collect(Collectors.toList());
                    }
                    List<String> columnFromPathList = Collections.emptyList();
                    if (StringUtils.isNoneBlank(taskInfo.getColumnFromPath())) {
                        columnFromPathList =
                                Arrays.stream(taskInfo.getColumnFromPath().split(",")).collect(Collectors.toList());
                    }
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
        }
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

    @VisibleForTesting
    public void checkMapping(EtlJobConfig.EtlTable etlTable,
                             Map<String, EtlJobConfig.EtlColumnMapping> columnMappingMap) throws SparkLoadException {
        Optional<EtlJobConfig.EtlIndex> baseIdx = etlTable.indexes.stream().filter(idx -> idx.isBaseIndex).findFirst();
        if (baseIdx.isPresent()) {
            EtlJobConfig.EtlIndex etlIndex = baseIdx.get();
            for (EtlJobConfig.EtlColumn column : etlIndex.columns) {
                if ("HLL".equalsIgnoreCase(column.columnType)) {
                    EtlJobConfig.EtlColumnMapping mapping = columnMappingMap.get(column.columnName);
                    checkHllMapping(column.columnName, mapping);
                }
                if ("BITMAP".equalsIgnoreCase(column.columnType)) {
                    EtlJobConfig.EtlColumnMapping mapping = columnMappingMap.get(column.columnName);
                    checkBitmapMapping(column.columnName, mapping);
                }
            }
        }
    }

    private void checkHllMapping(String columnName, EtlJobConfig.EtlColumnMapping mapping) throws SparkLoadException {
        if (mapping == null) {
            throw new SparkLoadException("");
        }
        Pattern pattern = Pattern.compile("(\\w+)\\(.*\\)");
        Matcher matcher = pattern.matcher(mapping.expr);
        if (matcher.find()) {
            if ("hll_hash".equalsIgnoreCase(matcher.group(1))
                    || "hll_empty".equalsIgnoreCase(matcher.group(1))) {
                return;
            }
            throw new SparkLoadException("HLL column must use hll function, like " + columnName + "=hll_hash(xxx) or "
                    + columnName + "=hll_empty()");
        }
    }

    private void checkBitmapMapping(String columnName, EtlJobConfig.EtlColumnMapping mapping)
            throws SparkLoadException {
        if (mapping == null) {
            throw new SparkLoadException("");
        }
        Pattern pattern = Pattern.compile("(\\w+)\\(.*\\)");
        Matcher matcher = pattern.matcher(mapping.expr);
        if (matcher.find()) {
            if ("to_bitmap".equalsIgnoreCase(matcher.group(1)) || "bitmap_hash".equalsIgnoreCase(matcher.group(1))
                    || "bitmap_dict".equalsIgnoreCase(matcher.group(1))
                    || "binary_bitmap".equalsIgnoreCase(matcher.group(1))) {
                return;
            }
            throw new SparkLoadException(
                    "BITMAP column must use bitmap function, like " + columnName + "=to_bitmap(xxx) or "
                            + columnName + "=bitmap_hash() or " + columnName + "=bitmap_dict() or "
                            + columnName + "=binary_bitmap()");
        }
    }

}
