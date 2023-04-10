package org.apache.doris.spark.format;

import org.apache.doris.spark.exception.StreamLoadException;
import org.apache.doris.spark.util.ListUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonDataFormat extends DataFormat{


    public JsonDataFormat(String[] dfColumns, Map<String, String> settings) {
        super(FormatEnum.json.name(),dfColumns, settings);
    }

    @Override
    public List<String> wtriteAsString(List<List<Object>> rows) throws Exception{
        List<Map<Object, Object>> dataList = new ArrayList<>();
        for (List<Object> row : rows) {
            Map<Object, Object> dataMap = new HashMap<>();
            // when created from CachedDorisStreamLoadClient, the dfColumns is null
            if (dfColumns!= null && dfColumns.length != row.size()) {
                throw new StreamLoadException("The number of configured columns does not match the number of data columns.");
            }
            for (int i = 0; i < dfColumns.length; i++) {
                dataMap.put(dfColumns[i], row.get(i));
            }
            dataList.add(dataMap);
        }
        // splits large collections to normal collection to avoid the "Requested array size exceeds VM limit" exception
        return ListUtils.getSerializedList(dataList);

    }


}
