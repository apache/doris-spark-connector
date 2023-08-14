package org.apache.doris.spark.util;

import junit.framework.TestCase;
import org.junit.Assert;
import scala.collection.mutable.WrappedArray;

import java.sql.Timestamp;
import java.util.Arrays;

public class DataUtilTest extends TestCase {

    public void testHandleColumnValue() {
        Assert.assertEquals("2023-08-14 18:00:00.0", DataUtil.handleColumnValue(Timestamp.valueOf("2023-08-14 18:00:00")));
        Assert.assertEquals("[1, 2, 3]", DataUtil.handleColumnValue(WrappedArray.make(new Integer[]{1,2,3})));
    }
}