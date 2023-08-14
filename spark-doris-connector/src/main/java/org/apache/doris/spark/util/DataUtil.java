package org.apache.doris.spark.util;

import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.sql.Timestamp;
import java.util.Arrays;

public class DataUtil {

    public static final String NULL_VALUE = "\\N";

    public static Object handleColumnValue(Object value) {

        if (value == null) {
            return NULL_VALUE;
        }

        if (value instanceof Timestamp) {
            return value.toString();
        }

        if (value instanceof WrappedArray) {

            Object[] arr = JavaConverters.seqAsJavaList((WrappedArray) value).toArray();
            return Arrays.toString(arr);
        }

        return value;

    }

}
