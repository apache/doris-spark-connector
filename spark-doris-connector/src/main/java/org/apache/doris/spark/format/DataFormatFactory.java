package org.apache.doris.spark.format;

import org.apache.doris.spark.exception.StreamLoadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class DataFormatFactory implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFormatFactory.class);

    public static DataFormat getDataFormat(String type,String[] dfColumns, Map<String,String> settings) throws StreamLoadException {
        DataFormat dataFormat;
        if(type.equalsIgnoreCase(FormatEnum.csv.name())){
            dataFormat = new CsvDataFormat(dfColumns,settings);
        }else if(type.equalsIgnoreCase(FormatEnum.json.name())){
            dataFormat = new JsonDataFormat(dfColumns,settings);
        }else{
            throw new StreamLoadException(String.format("Unsupported data format in stream load: %s.", type));
        }
        LOG.info("get {} data format",type);
        return dataFormat;
    }



}
