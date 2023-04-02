package org.apache.doris.spark.format;

import java.util.List;
import java.util.Map;

public abstract class DataFormat {

    protected String type;

    protected String[] dfColumns;

    protected Map<String, String> settings;

    public DataFormat(String type, String[] dfColumns, Map<String, String> settings) {
        this.type = type;
        this.dfColumns = dfColumns;
        this.settings = settings;
    }

    public abstract List<String> wtriteAsString(List<List<Object>> rows) throws Exception;

    public String getType() {
        return type;
    }
}
