package org.apache.doris.spark.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import junit.framework.TestCase;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import scala.collection.JavaConverters;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(JUnit4.class)
public class DataUtilTest extends TestCase {

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private List<Object> values;

    private StructType schema;

    @Override
    @Before
    public void setUp() throws Exception {
        if (values == null) {
            values = new LinkedList<>();
            values.add(1);
            values.add(null);
            values.add(UTF8String.fromString("abc"));
        }
        schema = new StructType(new StructField[]{
                StructField.apply("c1", DataTypes.IntegerType, true, Metadata.empty()),
                StructField.apply("c2", DataTypes.StringType, true, Metadata.empty()),
                StructField.apply("c3", DataTypes.StringType, true, Metadata.empty())
        });
    }


    @Test
    public void rowToCsvBytes() {
        InternalRow row = InternalRow.apply(JavaConverters.collectionAsScalaIterable(values).toSeq());
        byte[] bytes = DataUtil.rowToCsvBytes(row, schema, ",", false);
        Assert.assertArrayEquals("1,\\N,abc".getBytes(StandardCharsets.UTF_8), bytes);
        byte[] bytes1 = DataUtil.rowToCsvBytes(row, schema, ",", true);
        Assert.assertArrayEquals("\"1\",\"\\N\",\"abc\"".getBytes(StandardCharsets.UTF_8), bytes1);
    }

    @Test
    public void rowToJsonBytes() throws JsonProcessingException {
        Map<String, Object> dataMap = new HashMap<>(values.size());
        dataMap.put("c1", 1);
        dataMap.put("c2", null);
        dataMap.put("c3", "abc");
        InternalRow row = InternalRow.apply(JavaConverters.collectionAsScalaIterable(values).toSeq());
        byte[] bytes = DataUtil.rowToJsonBytes(row, schema);
        Assert.assertArrayEquals(MAPPER.writeValueAsBytes(dataMap), bytes);
    }

}