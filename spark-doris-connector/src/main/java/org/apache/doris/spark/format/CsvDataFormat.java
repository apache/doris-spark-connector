package org.apache.doris.spark.format;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvDataFormat extends DataFormat{

    private String FIELD_DELIMITER;
    private String LINE_DELIMITER;
    private String NULL_VALUE = "\\N";

    public CsvDataFormat(String[] dfColumns, Map<String, String> settings) {
        super(FormatEnum.csv.name(),dfColumns, settings);
        FIELD_DELIMITER = this.settings.getOrDefault("column_separator","\t");
        LINE_DELIMITER = this.settings.getOrDefault("line_delimiter","\n");
    }


    protected String listToString(List<List<Object>> rows) {
        return rows.stream().map(row ->
                row.stream().map(field ->
                        (field == null) ? NULL_VALUE : field.toString()
                ).collect(Collectors.joining(FIELD_DELIMITER))
        ).collect(Collectors.joining(LINE_DELIMITER));
    }

    @Override
    public List<String> wtriteAsString(List<List<Object>> rows) throws Exception {
        return Lists.newArrayList(listToString(rows));
    }
}
