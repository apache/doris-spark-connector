package org.apache.doris.spark.util;

import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

public class CopySQLBuilder {
    private final static String COPY_SYNC = "copy.async";
    private final static String FILE_TYPE = "file.type";
    private final String fileName;
    private Properties copyIntoProps;
    private String tableIdentifier;
    private String data_type;

    public CopySQLBuilder(String data_type, Properties copyIntoProps, String tableIdentifier, String fileName) {
        this.data_type = data_type;
        this.fileName = fileName;
        this.tableIdentifier = tableIdentifier;
        this.copyIntoProps = copyIntoProps;
    }

    public String buildCopySQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ").append(tableIdentifier).append(" FROM @~('{").append(String.format(fileName,"}*')")).append(" PROPERTIES (");

        //copy into must be sync
        copyIntoProps.put(COPY_SYNC, false);
        copyIntoProps.put(FILE_TYPE, data_type);
        if (data_type.equals("json")) {
            copyIntoProps.put("file.strip_outer_array", "true");
        }
        StringJoiner props = new StringJoiner(",");
        for (Map.Entry<Object, Object> entry : copyIntoProps.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            String prop = String.format("'%s'='%s'", key, value);
            props.add(prop);
        }
        sb.append(props).append(" )");
        return sb.toString();
    }
}