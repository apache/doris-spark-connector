// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.client.read;

import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TStatus;
import org.apache.doris.sdk.thrift.TStatusCode;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.rest.models.Schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.types.Decimal;
import static org.hamcrest.core.StringStartsWith.startsWith;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class RowBatchTest {

    private final static Logger logger = LoggerFactory.getLogger(RowBatchTest.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRowBatch() throws Exception {
        // schema
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k0", FieldType.nullable(new ArrowType.Bool()), null));
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Int(8, true)), null));
        childrenBuilder.add(new Field("k2", FieldType.nullable(new ArrowType.Int(16, true)), null));
        childrenBuilder.add(new Field("k3", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("k4", FieldType.nullable(new ArrowType.Int(64, true)), null));
        childrenBuilder.add(new Field("k9", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
        childrenBuilder.add(new Field("k8", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        childrenBuilder.add(new Field("k10", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k11", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k5", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k6", FieldType.nullable(new ArrowType.Utf8()), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k0");
        BitVector bitVector = (BitVector) vector;
        bitVector.setInitialCapacity(3);
        bitVector.allocateNew(3);
        bitVector.setSafe(0, 1);
        bitVector.setSafe(1, 0);
        bitVector.setSafe(2, 1);
        vector.setValueCount(3);

        vector = root.getVector("k1");
        TinyIntVector tinyIntVector = (TinyIntVector) vector;
        tinyIntVector.setInitialCapacity(3);
        tinyIntVector.allocateNew(3);
        tinyIntVector.setSafe(0, 1);
        tinyIntVector.setSafe(1, 2);
        tinyIntVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k2");
        SmallIntVector smallIntVector = (SmallIntVector) vector;
        smallIntVector.setInitialCapacity(3);
        smallIntVector.allocateNew(3);
        smallIntVector.setSafe(0, 1);
        smallIntVector.setSafe(1, 2);
        smallIntVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k3");
        IntVector intVector = (IntVector) vector;
        intVector.setInitialCapacity(3);
        intVector.allocateNew(3);
        intVector.setSafe(0, 1);
        intVector.setNull(1);
        intVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k4");
        BigIntVector bigIntVector = (BigIntVector) vector;
        bigIntVector.setInitialCapacity(3);
        bigIntVector.allocateNew(3);
        bigIntVector.setSafe(0, 1);
        bigIntVector.setSafe(1, 2);
        bigIntVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k5");
        VarCharVector varCharVector = (VarCharVector) vector;
        varCharVector.setInitialCapacity(3);
        varCharVector.allocateNew();
        varCharVector.setIndexDefined(0);
        varCharVector.setValueLengthSafe(0, 5);
        varCharVector.setSafe(0, "12.34".getBytes());
        varCharVector.setIndexDefined(1);
        varCharVector.setValueLengthSafe(1, 5);
        varCharVector.setSafe(1, "88.88".getBytes());
        varCharVector.setIndexDefined(2);
        varCharVector.setValueLengthSafe(2, 2);
        varCharVector.setSafe(2, "10".getBytes());
        vector.setValueCount(3);

        vector = root.getVector("k6");
        VarCharVector charVector = (VarCharVector) vector;
        charVector.setInitialCapacity(3);
        charVector.allocateNew();
        charVector.setIndexDefined(0);
        charVector.setValueLengthSafe(0, 5);
        charVector.setSafe(0, "char1".getBytes());
        charVector.setIndexDefined(1);
        charVector.setValueLengthSafe(1, 5);
        charVector.setSafe(1, "char2".getBytes());
        charVector.setIndexDefined(2);
        charVector.setValueLengthSafe(2, 5);
        charVector.setSafe(2, "char3".getBytes());
        vector.setValueCount(3);

        vector = root.getVector("k8");
        Float8Vector float8Vector = (Float8Vector) vector;
        float8Vector.setInitialCapacity(3);
        float8Vector.allocateNew(3);
        float8Vector.setSafe(0, 1.1);
        float8Vector.setSafe(1, 2.2);
        float8Vector.setSafe(2, 3.3);
        vector.setValueCount(3);

        vector = root.getVector("k9");
        Float4Vector float4Vector = (Float4Vector) vector;
        float4Vector.setInitialCapacity(3);
        float4Vector.allocateNew(3);
        float4Vector.setSafe(0, 1.1f);
        float4Vector.setSafe(1, 2.2f);
        float4Vector.setSafe(2, 3.3f);
        vector.setValueCount(3);

        vector = root.getVector("k10");
        VarCharVector datecharVector = (VarCharVector) vector;
        datecharVector.setInitialCapacity(3);
        datecharVector.allocateNew();
        datecharVector.setIndexDefined(0);
        datecharVector.setValueLengthSafe(0, 5);
        datecharVector.setSafe(0, "2008-08-08".getBytes());
        datecharVector.setIndexDefined(1);
        datecharVector.setValueLengthSafe(1, 5);
        datecharVector.setSafe(1, "1900-08-08".getBytes());
        datecharVector.setIndexDefined(2);
        datecharVector.setValueLengthSafe(2, 5);
        datecharVector.setSafe(2, "2100-08-08".getBytes());
        vector.setValueCount(3);

        vector = root.getVector("k11");
        VarCharVector timecharVector = (VarCharVector) vector;
        timecharVector.setInitialCapacity(3);
        timecharVector.allocateNew();
        timecharVector.setIndexDefined(0);
        timecharVector.setValueLengthSafe(0, 5);
        timecharVector.setSafe(0, "2008-08-08 00:00:00".getBytes());
        timecharVector.setIndexDefined(1);
        timecharVector.setValueLengthSafe(1, 5);
        timecharVector.setSafe(1, "1900-08-08 00:00:00".getBytes());
        timecharVector.setIndexDefined(2);
        timecharVector.setValueLengthSafe(2, 5);
        timecharVector.setSafe(2, "2100-08-08 00:00:00".getBytes());
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[{\"type\":\"BOOLEAN\",\"name\":\"k0\",\"comment\":\"\"},"
                + "{\"type\":\"TINYINT\",\"name\":\"k1\",\"comment\":\"\"},{\"type\":\"SMALLINT\",\"name\":\"k2\","
                + "\"comment\":\"\"},{\"type\":\"INT\",\"name\":\"k3\",\"comment\":\"\"},{\"type\":\"BIGINT\","
                + "\"name\":\"k4\",\"comment\":\"\"},{\"type\":\"FLOAT\",\"name\":\"k9\",\"comment\":\"\"},"
                + "{\"type\":\"DOUBLE\",\"name\":\"k8\",\"comment\":\"\"},{\"type\":\"DATE\",\"name\":\"k10\","
                + "\"comment\":\"\"},{\"type\":\"DATETIME\",\"name\":\"k11\",\"comment\":\"\"},"
                + "{\"name\":\"k5\",\"scale\":\"0\",\"comment\":\"\","
                + "\"type\":\"DECIMAL\",\"precision\":\"9\",\"aggregation_type\":\"\"},{\"type\":\"CHAR\",\"name\":\"k6\",\"comment\":\"\",\"aggregation_type\":\"REPLACE_IF_NOT_NULL\"}],"
                + "\"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        List<Object> expectedRow1 = Arrays.asList(
                Boolean.TRUE,
                (byte) 1,
                (short) 1,
                1,
                1L,
                (float) 1.1,
                (double) 1.1,
                Date.valueOf("2008-08-08"),
                Timestamp.valueOf("2008-08-08 00:00:00"),
                Decimal.apply(1234L, 4, 2),
                "char1"
        );

        List<Object> expectedRow2 = Arrays.asList(
                Boolean.FALSE,
                (byte) 2,
                (short) 2,
                null,
                2L,
                (float) 2.2,
                (double) 2.2,
                Date.valueOf("1900-08-08"),
                Timestamp.valueOf("1900-08-08 00:00:00"),
                Decimal.apply(8888L, 4, 2),
                "char2"
        );

        List<Object> expectedRow3 = Arrays.asList(
                Boolean.TRUE,
                (byte) 3,
                (short) 3,
                3,
                3L,
                (float) 3.3,
                (double) 3.3,
                Date.valueOf("2100-08-08"),
                Timestamp.valueOf("2100-08-08 00:00:00"),
                Decimal.apply(10L, 2, 0),
                "char3"
        );

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        Assert.assertArrayEquals(expectedRow1.toArray(), actualRow1.toArray());

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        Assert.assertArrayEquals(expectedRow2.toArray(), actualRow2.toArray());

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow3 = rowBatch.next();
        Assert.assertArrayEquals(expectedRow3.toArray(), actualRow3.toArray());

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testBinary() throws Exception {
        byte[] binaryRow0 = {'a', 'b', 'c'};
        byte[] binaryRow1 = {'d', 'e', 'f'};
        byte[] binaryRow2 = {'g', 'h', 'i'};

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k7", FieldType.nullable(new ArrowType.Binary()), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k7");
        VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
        varBinaryVector.setInitialCapacity(3);
        varBinaryVector.allocateNew();
        varBinaryVector.setIndexDefined(0);
        varBinaryVector.setValueLengthSafe(0, 3);
        varBinaryVector.setSafe(0, binaryRow0);
        varBinaryVector.setIndexDefined(1);
        varBinaryVector.setValueLengthSafe(1, 3);
        varBinaryVector.setSafe(1, binaryRow1);
        varBinaryVector.setIndexDefined(2);
        varBinaryVector.setValueLengthSafe(2, 3);
        varBinaryVector.setSafe(2, binaryRow2);
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[{\"type\":\"BINARY\",\"name\":\"k7\",\"comment\":\"\"}], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertArrayEquals(binaryRow0, (byte[]) actualRow0.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        Assert.assertArrayEquals(binaryRow1, (byte[]) actualRow1.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        Assert.assertArrayEquals(binaryRow2, (byte[]) actualRow2.get(0));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testDecimalV2() throws Exception {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k7", FieldType.nullable(new ArrowType.Decimal(27, 9)), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k7");
        DecimalVector decimalVector = (DecimalVector) vector;
        decimalVector.setInitialCapacity(3);
        decimalVector.allocateNew(3);
        decimalVector.setSafe(0, new BigDecimal("12.340000000"));
        decimalVector.setSafe(1, new BigDecimal("88.880000000"));
        decimalVector.setSafe(2, new BigDecimal("10.000000000"));
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[{\"type\":\"DECIMALV2\",\"scale\": 0,"
                + "\"precision\": 9, \"name\":\"k7\",\"comment\":\"\"}], "
                + "\"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertEquals(Decimal.apply(12340000000L, 11, 9), (Decimal) actualRow0.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        Assert.assertEquals(Decimal.apply(88880000000L, 11, 9), (Decimal) actualRow1.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        Assert.assertEquals(Decimal.apply(10000000000L, 11, 9), (Decimal) actualRow2.get(0));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testDate() throws DorisException, IOException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k2", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k3", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        FieldVector vector = root.getVector("k1");
        VarCharVector dateVector = (VarCharVector) vector;
        dateVector.setInitialCapacity(1);
        dateVector.allocateNew();
        dateVector.setIndexDefined(0);
        dateVector.setValueLengthSafe(0, 10);
        dateVector.setSafe(0, "2023-08-09".getBytes());
        vector.setValueCount(1);


        vector = root.getVector("k2");
        VarCharVector dateV2Vector = (VarCharVector) vector;
        dateV2Vector.setInitialCapacity(1);
        dateV2Vector.allocateNew();
        dateV2Vector.setIndexDefined(0);
        dateV2Vector.setValueLengthSafe(0, 10);
        dateV2Vector.setSafe(0, "2023-08-10".getBytes());
        vector.setValueCount(1);

        vector = root.getVector("k3");
        DateDayVector dateNewVector = (DateDayVector) vector;
        dateNewVector.setInitialCapacity(1);
        dateNewVector.allocateNew();
        dateNewVector.setIndexDefined(0);
        dateNewVector.setSafe(0, 19802);
        vector.setValueCount(1);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());


        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"DATE\",\"name\":\"k1\",\"comment\":\"\"}, " +
                "{\"type\":\"DATEV2\",\"name\":\"k2\",\"comment\":\"\"}, " +
                "{\"type\":\"DATEV2\",\"name\":\"k3\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertEquals(Date.valueOf("2023-08-09"), actualRow0.get(0));
        Assert.assertEquals(Date.valueOf("2023-08-10"), actualRow0.get(1));
        Assert.assertEquals(Date.valueOf("2024-03-20"), actualRow0.get(2));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();

    }

    @Test
    public void testLargeInt() throws DorisException, IOException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k2", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        FieldVector vector = root.getVector("k1");
        VarCharVector lageIntVector = (VarCharVector) vector;
        lageIntVector.setInitialCapacity(1);
        lageIntVector.allocateNew();
        lageIntVector.setIndexDefined(0);
        lageIntVector.setValueLengthSafe(0, 19);
        lageIntVector.setSafe(0, "9223372036854775808".getBytes());
        vector.setValueCount(1);


        vector = root.getVector("k2");
        FixedSizeBinaryVector lageIntVector1 = (FixedSizeBinaryVector) vector;
        lageIntVector1.setInitialCapacity(1);
        lageIntVector1.allocateNew();
        lageIntVector1.setIndexDefined(0);
        byte[] bytes = new BigInteger("9223372036854775809").toByteArray();
        byte[] fixedBytes = new byte[16];
        System.arraycopy(bytes, 0, fixedBytes, 16 - bytes.length, bytes.length);
        ArrayUtils.reverse(fixedBytes);
        lageIntVector1.setSafe(0, fixedBytes);
        vector.setValueCount(1);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"LARGEINT\",\"name\":\"k1\",\"comment\":\"\"}, " +
                "{\"type\":\"LARGEINT\",\"name\":\"k2\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();

        Assert.assertEquals("9223372036854775808", actualRow0.get(0));
        Assert.assertEquals("9223372036854775809", actualRow0.get(1));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();

    }

    @Test
    public void testMap() throws IOException, DorisException {

        ImmutableList<Field> mapChildren = ImmutableList.of(
                new Field("child", new FieldType(false, new ArrowType.Struct(), null),
                        ImmutableList.of(
                                new Field("key", new FieldType(false, new ArrowType.Utf8(), null), null),
                                new Field("value", new FieldType(false, new ArrowType.Int(32, true), null),
                                        null)
                        )
                ));

        ImmutableList<Field> fields = ImmutableList.of(
                new Field("col_map", new FieldType(false, new ArrowType.Map(false), null),
                        mapChildren)
        );

        RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(fields, null), allocator);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        MapVector mapVector = (MapVector) root.getVector("col_map");
        mapVector.allocateNew();
        UnionMapWriter mapWriter = mapVector.getWriter();
        for (int i = 0; i < 3; i++) {
            mapWriter.setPosition(i);
            mapWriter.startMap();
            mapWriter.startEntry();
            String key = "k" + (i + 1);
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            ArrowBuf buffer = allocator.buffer(bytes.length);
            buffer.setBytes(0, bytes);
            mapWriter.key().varChar().writeVarChar(0, bytes.length, buffer);
            buffer.close();
            mapWriter.value().integer().writeInt(i);
            mapWriter.endEntry();
            mapWriter.endMap();
        }
        mapWriter.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[{\"type\":\"MAP\",\"name\":\"col_map\",\"comment\":\"\"}" +
                "], \"status\":200}";


        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);
        Assert.assertTrue(rowBatch.hasNext());
        Assert.assertEquals(ImmutableMap.of("k1", "0"), rowBatch.next().get(0));
        Assert.assertTrue(rowBatch.hasNext());
        Assert.assertEquals(ImmutableMap.of("k2", "1"), rowBatch.next().get(0));
        Assert.assertTrue(rowBatch.hasNext());
        Assert.assertEquals(ImmutableMap.of("k3", "2"), rowBatch.next().get(0));
        Assert.assertFalse(rowBatch.hasNext());

    }

    @Test
    public void testStruct() throws IOException, DorisException {

        ImmutableList<Field> fields = ImmutableList.of(
                new Field("col_struct", new FieldType(false, new ArrowType.Struct(), null),
                        ImmutableList.of(new Field("a", new FieldType(false, new ArrowType.Utf8(), null), null),
                                new Field("b", new FieldType(false, new ArrowType.Int(32, true), null), null))
                ));

        RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(fields, null), allocator);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        StructVector structVector = (StructVector) root.getVector("col_struct");
        structVector.allocateNew();
        NullableStructWriter writer = structVector.getWriter();
        writer.setPosition(0);
        writer.start();
        byte[] bytes = "a1".getBytes(StandardCharsets.UTF_8);
        ArrowBuf buffer = allocator.buffer(bytes.length);
        buffer.setBytes(0, bytes);
        writer.varChar("a").writeVarChar(0, bytes.length, buffer);
        buffer.close();
        writer.integer("b").writeInt(1);
        writer.end();
        writer.setValueCount(1);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[{\"type\":\"STRUCT\",\"name\":\"col_struct\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);
        Assert.assertTrue(rowBatch.hasNext());
        Assert.assertEquals("{\"a\":\"a1\",\"b\":1}", rowBatch.next().get(0));

    }

    @Test
    public void testDateTime() throws IOException, DorisException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k2", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND,
                null)), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k1");
        VarCharVector datetimeVector = (VarCharVector) vector;
        datetimeVector.setInitialCapacity(3);
        datetimeVector.allocateNew();
        datetimeVector.setIndexDefined(0);
        datetimeVector.setValueLengthSafe(0, 20);
        datetimeVector.setSafe(0, "2024-03-20 00:00:00".getBytes());
        datetimeVector.setIndexDefined(1);
        datetimeVector.setValueLengthSafe(1, 20);
        datetimeVector.setSafe(1, "2024-03-20 00:00:01".getBytes());
        datetimeVector.setIndexDefined(2);
        datetimeVector.setValueLengthSafe(2, 20);
        datetimeVector.setSafe(2, "2024-03-20 00:00:02".getBytes());
        vector.setValueCount(3);

        LocalDateTime localDateTime = LocalDateTime.of(2024, 3, 20,
                0, 0, 0, 123456000);
        long second = localDateTime.atZone(ZoneId.systemDefault()).toEpochSecond();
        int nano = localDateTime.getNano();

        vector = root.getVector("k2");
        TimeStampMicroVector datetimeV2Vector = (TimeStampMicroVector) vector;
        datetimeV2Vector.setInitialCapacity(3);
        datetimeV2Vector.allocateNew();
        datetimeV2Vector.setIndexDefined(0);
        datetimeV2Vector.setSafe(0, second);
        datetimeV2Vector.setIndexDefined(1);
        datetimeV2Vector.setSafe(1, second * 1000 + nano / 1000000);
        datetimeV2Vector.setIndexDefined(2);
        datetimeV2Vector.setSafe(2, second * 1000000 + nano / 1000);
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());


        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"DATETIME\",\"name\":\"k1\",\"comment\":\"\"}, " +
                "{\"type\":\"DATETIMEV2\",\"name\":\"k2\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertEquals(Timestamp.valueOf("2024-03-20 00:00:00"), actualRow0.get(0));
        Assert.assertEquals(Timestamp.valueOf("2024-03-20 00:00:00"), actualRow0.get(1));

        List<Object> actualRow1 = rowBatch.next();
        Assert.assertEquals(Timestamp.valueOf("2024-03-20 00:00:01"), actualRow1.get(0));
        Assert.assertEquals(Timestamp.valueOf("2024-03-20 00:00:00.123"), actualRow1.get(1));

        List<Object> actualRow2 = rowBatch.next();
        Assert.assertEquals(Timestamp.valueOf("2024-03-20 00:00:02"), actualRow2.get(0));
        Assert.assertEquals(Timestamp.valueOf("2024-03-20 00:00:00.123456"), actualRow2.get(1));


        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();

    }

    @Test
    public void testVariant() throws DorisException, IOException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k1");
        VarCharVector datetimeVector = (VarCharVector) vector;
        datetimeVector.setInitialCapacity(3);
        datetimeVector.allocateNew();
        datetimeVector.setIndexDefined(0);
        datetimeVector.setValueLengthSafe(0, 20);
        datetimeVector.setSafe(0, "{\"id\":\"a\"}".getBytes());
        datetimeVector.setIndexDefined(1);
        datetimeVector.setValueLengthSafe(1, 20);
        datetimeVector.setSafe(1, "1000".getBytes());
        datetimeVector.setIndexDefined(2);
        datetimeVector.setValueLengthSafe(2, 20);
        datetimeVector.setSafe(2, "123.456".getBytes());
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());


        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"VARIANT\",\"name\":\"k\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();

        Assert.assertEquals("{\"id\":\"a\"}", actualRow0.get(0));

        List<Object> actualRow1 = rowBatch.next();
        Assert.assertEquals("1000", actualRow1.get(0));

        List<Object> actualRow2 = rowBatch.next();
        Assert.assertEquals("123.456", actualRow2.get(0));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();

    }

    @Test
    public void testIPv4() throws DorisException, IOException {

        ImmutableList<Field> fields =
                ImmutableList.of(
                        new Field("k1", FieldType.nullable(new ArrowType.Int(32, false)), null),
                        new Field("k2", FieldType.nullable(new ArrowType.Int(32, true)), null),
                        new Field("k3", FieldType.nullable(new ArrowType.Utf8()), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(fields, null),
                        new RootAllocator(Integer.MAX_VALUE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(5);

        long[] ipValues = {0, 255, 65535, 16777215, 4294967295L};
        String[] ipStrings = {
                "0.0.0.0", "0.0.0.255", "0.0.255.255", "0.255.255.255", "255.255.255.255"
        };

        UInt4Vector uInt4Vector = (UInt4Vector) root.getVector("k1");
        uInt4Vector.setInitialCapacity(5);
        uInt4Vector.allocateNew(5); // Fixed from 4 to 5 to match actual row count

        IntVector intVector = (IntVector) root.getVector("k2");
        intVector.setInitialCapacity(5);
        intVector.allocateNew(5); // Fixed from 4 to 5 to match actual row count

        VarCharVector varCharVector = (VarCharVector) root.getVector("k3");
        varCharVector.setInitialCapacity(5);
        varCharVector.allocateNew(5); // Fixed from 4 to 5 to match actual row count

        for (int i = 0; i < 5; i++) {
            uInt4Vector.setIndexDefined(i);
            if (i < 4) {
                uInt4Vector.setSafe(i, (int) ipValues[i]);
            } else {
                uInt4Vector.setWithPossibleTruncate(
                        i, ipValues[i]); // Large value that might be truncated
            }

            intVector.setIndexDefined(i);
            if (i < 4) {
                intVector.setSafe(i, (int) ipValues[i]);
            } else {
                intVector.setWithPossibleTruncate(
                        i, ipValues[i]); // Large value that might be truncated
            }

            varCharVector.setIndexDefined(i);
            byte[] bytes = ipStrings[i].getBytes(StandardCharsets.UTF_8);
            varCharVector.setSafe(i, bytes, 0, bytes.length);
        }

        uInt4Vector.setValueCount(5);
        intVector.setValueCount(5);
        varCharVector.setValueCount(5);
        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr =
                "{\"properties\":["
                        + "{\"type\":\"IPV4\",\"name\":\"k1\",\"comment\":\"\"},"
                        + "{\"type\":\"IPV4\",\"name\":\"k2\",\"comment\":\"\"},"
                        + "{\"type\":\"IPV4\",\"name\":\"k3\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        // Validate each row of data
        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(
                    "Expected row " + i + " to exist, but it doesn't", rowBatch.hasNext());
            List<Object> actualRow = rowBatch.next();
            assertEquals(ipStrings[i], actualRow.get(0));
            assertEquals(ipStrings[i], actualRow.get(1));
            assertEquals(ipStrings[i], actualRow.get(2));
        }

        // Ensure no more rows exist
        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testIPv6() throws DorisException, IOException {
        ImmutableList<Field> childrenFields =
                ImmutableList.of(
                        new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("k2", FieldType.nullable(new ArrowType.Utf8()), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(childrenFields, null),
                        new RootAllocator(Integer.MAX_VALUE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(13);

        VarCharVector ipv6Vector = (VarCharVector) root.getVector("k1");
        VarCharVector ipv6Vector1 = (VarCharVector) root.getVector("k2");
        ipv6Vector.setInitialCapacity(13);
        ipv6Vector.allocateNew();
        ipv6Vector1.setInitialCapacity(13);
        ipv6Vector1.allocateNew(13);

        String[] k1Values = {
                "0",
                "1",
                "65535",
                "65536",
                "4294967295",
                "4294967296",
                "8589934591",
                "281470681743359",
                "281470681743360",
                "281474976710655",
                "281474976710656",
                "340277174624079928635746639885392347137",
                "340282366920938463463374607431768211455"
        };

        String[] k2Values = {
                "::",
                "::1",
                "::ffff",
                "::0.1.0.0",
                "::255.255.255.255",
                "::1:0:0",
                "::1:ffff:ffff",
                "::fffe:ffff:ffff",
                "::ffff:0.0.0.0",
                "::ffff:255.255.255.255",
                "::1:0:0:0",
                "ffff::1:ffff:ffff:1",
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"
        };

        for (int i = 0; i < 13; i++) {
            ipv6Vector.setIndexDefined(i);
            ipv6Vector.setSafe(i, k1Values[i].getBytes());

            ipv6Vector1.setIndexDefined(i);
            ipv6Vector1.setSafe(i, k2Values[i].getBytes());
        }

        ipv6Vector.setValueCount(13);
        ipv6Vector1.setValueCount(13);
        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr =
                "{\"properties\":["
                        + "{\"type\":\"IPV6\",\"name\":\"k1\",\"comment\":\"\"},"
                        + "{\"type\":\"IPV6\",\"name\":\"k2\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        String[][] expectedResults = {
                {"::", "::"},
                {"::1", "::1"},
                {"::ffff", "::ffff"},
                {"::0.1.0.0", "::0.1.0.0"},
                {"::255.255.255.255", "::255.255.255.255"},
                {"::1:0:0", "::1:0:0"},
                {"::1:ffff:ffff", "::1:ffff:ffff"},
                {"::fffe:ffff:ffff", "::fffe:ffff:ffff"},
                {"::ffff:0.0.0.0", "::ffff:0.0.0.0"},
                {"::ffff:255.255.255.255", "::ffff:255.255.255.255"},
                {"::1:0:0:0", "::1:0:0:0"},
                {"ffff::1:ffff:ffff:1", "ffff::1:ffff:ffff:1"},
                {"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"}
        };

        for (String[] expectedResult : expectedResults) {
            Assert.assertTrue(rowBatch.hasNext());
            List<Object> actualRow = rowBatch.next();
            assertEquals(expectedResult[0], actualRow.get(0));
            assertEquals(expectedResult[1], actualRow.get(1));
        }

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testDatetimeJava8API() throws DorisException, IOException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k0", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null));
        childrenBuilder.add(new Field("k2", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND,
                null)), null));
        childrenBuilder.add(new Field("k3", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND,
                null)), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        FieldVector vector = root.getVector("k0");
        VarCharVector dateVector = (VarCharVector) vector;
        dateVector.setInitialCapacity(1);
        dateVector.allocateNew();
        dateVector.setIndexDefined(0);
        dateVector.setValueLengthSafe(0, 20);
        dateVector.setSafe(0, "2025-01-01".getBytes());
        vector.setValueCount(1);

        LocalDate localDate = LocalDate.of(2025, 2, 1);
        long date = localDate.toEpochDay();

        vector = root.getVector("k1");
        DateDayVector date2Vector = (DateDayVector) vector;
        date2Vector.setInitialCapacity(1);
        date2Vector.allocateNew();
        date2Vector.setIndexDefined(0);
        date2Vector.setSafe(0, (int) date);
        vector.setValueCount(1);

        LocalDateTime localDateTime = LocalDateTime.of(2025, 2, 24,
                0, 0, 0, 123000000);
        long second = localDateTime.atZone(ZoneId.systemDefault()).toEpochSecond();
        int nano = localDateTime.getNano();

        vector = root.getVector("k2");
        TimeStampMicroVector datetimeV2Vector = (TimeStampMicroVector) vector;
        datetimeV2Vector.setInitialCapacity(1);
        datetimeV2Vector.allocateNew();
        datetimeV2Vector.setIndexDefined(0);
        datetimeV2Vector.setSafe(0, second * 1000000 + nano / 1000);
        vector.setValueCount(1);

        LocalDateTime localDateTime1 = LocalDateTime.of(2025, 2, 24,
                1, 2, 3, 123456000);
        long second1 = localDateTime1.atZone(ZoneId.systemDefault()).toEpochSecond();
        int nano1 = localDateTime1.getNano();

        vector = root.getVector("k3");
        TimeStampMicroVector datetimeV2Vector1 = (TimeStampMicroVector) vector;
        datetimeV2Vector1.setInitialCapacity(1);
        datetimeV2Vector1.allocateNew();
        datetimeV2Vector1.setIndexDefined(0);
        datetimeV2Vector1.setSafe(0, second1 * 1000000 + nano1 / 1000);
        vector.setValueCount(1);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());


        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"DATE\",\"name\":\"k0\",\"comment\":\"\"}, " +
                "{\"type\":\"DATEV2\",\"name\":\"k1\",\"comment\":\"\"}," +
                "{\"type\":\"DATETIME\",\"name\":\"k2\",\"comment\":\"\"}," +
                "{\"type\":\"DATETIMEV2\",\"name\":\"k3\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);

        RowBatch rowBatch1 = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch1.hasNext());
        List<Object> actualRow0 = rowBatch1.next();
        Assert.assertEquals(Date.valueOf("2025-01-01"), actualRow0.get(0));
        Assert.assertEquals(Date.valueOf("2025-02-01"), actualRow0.get(1));
        Assert.assertEquals(Timestamp.valueOf("2025-02-24 00:00:00.123"), actualRow0.get(2));
        Assert.assertEquals(Timestamp.valueOf("2025-02-24 01:02:03.123456"), actualRow0.get(3));

        Assert.assertFalse(rowBatch1.hasNext());

        RowBatch rowBatch2 = new RowBatch(scanBatchResult, schema, true);

        Assert.assertTrue(rowBatch2.hasNext());
        List<Object> actualRow01 = rowBatch2.next();
        Assert.assertEquals(LocalDate.of(2025,1,1), actualRow01.get(0));
        Assert.assertEquals(localDate, actualRow01.get(1));
        Assert.assertEquals(localDateTime.atZone(ZoneId.systemDefault()).toInstant(), actualRow01.get(2));
        Assert.assertEquals(localDateTime1.atZone(ZoneId.systemDefault()).toInstant(), actualRow01.get(3));

        Assert.assertFalse(rowBatch2.hasNext());

    }

    @Test
    public void testArrayTypeInt() throws DorisException, IOException {
        // Test ARRAY<INT> type conversion
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        
        // Create nested field for array element (INT)
        Field elementField = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field arrayField = new Field("arr_int", FieldType.nullable(ArrowType.List.INSTANCE), 
                ImmutableList.of(elementField));
        
        childrenBuilder.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(arrayField);

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(2);

        // Set id field
        IntVector idVector = (IntVector) root.getVector("id");
        idVector.setInitialCapacity(2);
        idVector.allocateNew();
        idVector.setSafe(0, 1);
        idVector.setSafe(1, 2);
        idVector.setValueCount(2);

        // Set array field
        ListVector listVector = (ListVector) root.getVector("arr_int");
        listVector.setInitialCapacity(2);
        listVector.allocateNew();
        
        // First row: [1, 2, 3]
        listVector.startNewValue(0);
        IntVector dataVector = (IntVector) listVector.getDataVector();
        dataVector.setSafe(0, 1);
        dataVector.setSafe(1, 2);
        dataVector.setSafe(2, 3);
        listVector.endValue(0, 3);
        
        // Second row: [4, 5]
        listVector.startNewValue(1);
        dataVector.setSafe(3, 4);
        dataVector.setSafe(4, 5);
        listVector.endValue(1, 2);
        
        dataVector.setValueCount(5);
        listVector.setValueCount(2);

        arrowStreamWriter.writeBatch();
        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"INT\",\"name\":\"id\",\"comment\":\"\"}," +
                "{\"type\":\"ARRAY\",\"name\":\"arr_int\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);
        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> row1 = rowBatch.next();
        Assert.assertEquals(1, row1.get(0));
        
        @SuppressWarnings("unchecked")
        List<Object> arr1 = (List<Object>) row1.get(1);
        Assert.assertNotNull(arr1);
        Assert.assertEquals(3, arr1.size());
        Assert.assertEquals(1, arr1.get(0));
        Assert.assertEquals(2, arr1.get(1));
        Assert.assertEquals(3, arr1.get(2));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> row2 = rowBatch.next();
        Assert.assertEquals(2, row2.get(0));
        
        @SuppressWarnings("unchecked")
        List<Object> arr2 = (List<Object>) row2.get(1);
        Assert.assertNotNull(arr2);
        Assert.assertEquals(2, arr2.size());
        Assert.assertEquals(4, arr2.get(0));
        Assert.assertEquals(5, arr2.get(1));

        Assert.assertFalse(rowBatch.hasNext());
        
        root.close();
    }

    @Test
    public void testArrayTypeString() throws DorisException, IOException {
        // Test ARRAY<STRING> type conversion
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        
        Field elementField = new Field("element", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field arrayField = new Field("arr_string", FieldType.nullable(ArrowType.List.INSTANCE), 
                ImmutableList.of(elementField));
        
        childrenBuilder.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(arrayField);

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        IntVector idVector = (IntVector) root.getVector("id");
        idVector.setInitialCapacity(1);
        idVector.allocateNew();
        idVector.setSafe(0, 1);
        idVector.setValueCount(1);

        ListVector listVector = (ListVector) root.getVector("arr_string");
        listVector.setInitialCapacity(1);
        listVector.allocateNew();
        
        listVector.startNewValue(0);
        VarCharVector dataVector = (VarCharVector) listVector.getDataVector();
        dataVector.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
        dataVector.setSafe(1, "world".getBytes(StandardCharsets.UTF_8));
        listVector.endValue(0, 2);
        
        dataVector.setValueCount(2);
        listVector.setValueCount(1);

        arrowStreamWriter.writeBatch();
        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"INT\",\"name\":\"id\",\"comment\":\"\"}," +
                "{\"type\":\"ARRAY\",\"name\":\"arr_string\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);
        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> row = rowBatch.next();
        Assert.assertEquals(1, row.get(0));
        
        // Array<String> (VARCHAR) is serialized as JSON string for performance optimization
        String jsonString = (String) row.get(1);
        Assert.assertNotNull(jsonString);
        // Verify it's valid JSON format
        Assert.assertTrue(jsonString.startsWith("["));
        Assert.assertTrue(jsonString.endsWith("]"));
        Assert.assertTrue(jsonString.contains("hello"));
        Assert.assertTrue(jsonString.contains("world"));

        Assert.assertFalse(rowBatch.hasNext());
        
        root.close();
    }

    @Test
    public void testArrayTypeNull() throws DorisException, IOException {
        // Test ARRAY with NULL values
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        
        Field elementField = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field arrayField = new Field("arr_int", FieldType.nullable(ArrowType.List.INSTANCE), 
                ImmutableList.of(elementField));
        
        childrenBuilder.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(arrayField);

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        IntVector idVector = (IntVector) root.getVector("id");
        idVector.setInitialCapacity(3);
        idVector.allocateNew();
        idVector.setSafe(0, 1);
        idVector.setSafe(1, 2);
        idVector.setSafe(2, 3);
        idVector.setValueCount(3);

        ListVector listVector = (ListVector) root.getVector("arr_int");
        listVector.setInitialCapacity(3);
        listVector.allocateNew();
        
        // First row: [1, 2]
        listVector.startNewValue(0);
        IntVector dataVector = (IntVector) listVector.getDataVector();
        dataVector.setSafe(0, 1);
        dataVector.setSafe(1, 2);
        listVector.endValue(0, 2);
        
        // Second row: NULL array
        listVector.setNull(1);
        
        // Third row: [3, null, 4] - array with null element
        listVector.startNewValue(2);
        dataVector.setSafe(2, 3);
        dataVector.setNull(3); // null element
        dataVector.setSafe(4, 4);
        listVector.endValue(2, 3);
        
        dataVector.setValueCount(5);
        listVector.setValueCount(3);

        arrowStreamWriter.writeBatch();
        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"INT\",\"name\":\"id\",\"comment\":\"\"}," +
                "{\"type\":\"ARRAY\",\"name\":\"arr_int\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);
        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        // First row
        Assert.assertTrue(rowBatch.hasNext());
        List<Object> row1 = rowBatch.next();
        @SuppressWarnings("unchecked")
        List<Object> arr1 = (List<Object>) row1.get(1);
        Assert.assertNotNull(arr1);
        Assert.assertEquals(2, arr1.size());

        // Second row - NULL array
        Assert.assertTrue(rowBatch.hasNext());
        List<Object> row2 = rowBatch.next();
        Assert.assertNull(row2.get(1));

        // Third row - array with null element
        Assert.assertTrue(rowBatch.hasNext());
        List<Object> row3 = rowBatch.next();
        @SuppressWarnings("unchecked")
        List<Object> arr3 = (List<Object>) row3.get(1);
        Assert.assertNotNull(arr3);
        Assert.assertEquals(3, arr3.size());
        Assert.assertEquals(3, arr3.get(0));
        Assert.assertNull(arr3.get(1)); // null element
        Assert.assertEquals(4, arr3.get(2));

        Assert.assertFalse(rowBatch.hasNext());
        
        root.close();
    }

    @Test
    public void testArrayTypeNested() throws DorisException, IOException {
        // Test ARRAY<ARRAY<INT>> nested array
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        
        // Inner array element (INT)
        Field innerElementField = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field innerArrayField = new Field("element", FieldType.nullable(ArrowType.List.INSTANCE), 
                ImmutableList.of(innerElementField));
        
        // Outer array element (ARRAY<INT>)
        Field outerArrayField = new Field("arr_nested", FieldType.nullable(ArrowType.List.INSTANCE), 
                ImmutableList.of(innerArrayField));
        
        childrenBuilder.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(outerArrayField);

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        IntVector idVector = (IntVector) root.getVector("id");
        idVector.setInitialCapacity(1);
        idVector.allocateNew();
        idVector.setSafe(0, 1);
        idVector.setValueCount(1);

        ListVector outerListVector = (ListVector) root.getVector("arr_nested");
        outerListVector.setInitialCapacity(1);
        outerListVector.allocateNew();
        
        // Create nested array: [[1, 2], [3, 4, 5]]
        outerListVector.startNewValue(0);
        
        // First inner array [1, 2]
        ListVector innerListVector = (ListVector) outerListVector.getDataVector();
        innerListVector.startNewValue(0);
        IntVector dataVector = (IntVector) innerListVector.getDataVector();
        dataVector.setSafe(0, 1);
        dataVector.setSafe(1, 2);
        innerListVector.endValue(0, 2);
        
        // Second inner array [3, 4, 5]
        innerListVector.startNewValue(1);
        dataVector.setSafe(2, 3);
        dataVector.setSafe(3, 4);
        dataVector.setSafe(4, 5);
        innerListVector.endValue(1, 3);
        
        outerListVector.endValue(0, 2);
        
        dataVector.setValueCount(5);
        innerListVector.setValueCount(2);
        outerListVector.setValueCount(1);

        arrowStreamWriter.writeBatch();
        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"INT\",\"name\":\"id\",\"comment\":\"\"}," +
                "{\"type\":\"ARRAY\",\"name\":\"arr_nested\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);
        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> row = rowBatch.next();
        Assert.assertEquals(1, row.get(0));
        
        // Array<Array<Int>> (nested LIST) is serialized as JSON string for performance optimization
        String jsonString = (String) row.get(1);
        Assert.assertNotNull(jsonString);
        // Verify it's valid JSON format for nested array
        Assert.assertTrue(jsonString.startsWith("[["));
        Assert.assertTrue(jsonString.endsWith("]]"));
        // Verify it contains the expected values
        Assert.assertTrue(jsonString.contains("1"));
        Assert.assertTrue(jsonString.contains("2"));
        Assert.assertTrue(jsonString.contains("3"));
        Assert.assertTrue(jsonString.contains("4"));
        Assert.assertTrue(jsonString.contains("5"));

        Assert.assertFalse(rowBatch.hasNext());
        
        root.close();
    }

    @Test
    public void testArrayTypeEmpty() throws DorisException, IOException {
        // Test empty array
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        
        Field elementField = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field arrayField = new Field("arr_int", FieldType.nullable(ArrowType.List.INSTANCE), 
                ImmutableList.of(elementField));
        
        childrenBuilder.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(arrayField);

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        IntVector idVector = (IntVector) root.getVector("id");
        idVector.setInitialCapacity(1);
        idVector.allocateNew();
        idVector.setSafe(0, 1);
        idVector.setValueCount(1);

        ListVector listVector = (ListVector) root.getVector("arr_int");
        listVector.setInitialCapacity(1);
        listVector.allocateNew();
        
        // Empty array
        listVector.startNewValue(0);
        listVector.endValue(0, 0);
        
        listVector.setValueCount(1);

        arrowStreamWriter.writeBatch();
        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"INT\",\"name\":\"id\",\"comment\":\"\"}," +
                "{\"type\":\"ARRAY\",\"name\":\"arr_int\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);
        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> row = rowBatch.next();
        Assert.assertEquals(1, row.get(0));
        
        @SuppressWarnings("unchecked")
        List<Object> arr = (List<Object>) row.get(1);
        Assert.assertNotNull(arr);
        Assert.assertEquals(0, arr.size()); // Empty array

        Assert.assertFalse(rowBatch.hasNext());
        
        root.close();
    }

    @Test
    public void testArrayTypeInference() throws DorisException, IOException {
        // Test that array element types are correctly inferred from Arrow Field
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        
        Field elementField = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field arrayField = new Field("arr_int", FieldType.nullable(ArrowType.List.INSTANCE), 
                ImmutableList.of(elementField));
        
        childrenBuilder.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(arrayField);

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        IntVector idVector = (IntVector) root.getVector("id");
        idVector.setInitialCapacity(1);
        idVector.allocateNew();
        idVector.setSafe(0, 1);
        idVector.setValueCount(1);

        ListVector listVector = (ListVector) root.getVector("arr_int");
        listVector.setInitialCapacity(1);
        listVector.allocateNew();
        
        listVector.startNewValue(0);
        IntVector dataVector = (IntVector) listVector.getDataVector();
        dataVector.setSafe(0, 100);
        dataVector.setSafe(1, 200);
        listVector.endValue(0, 2);
        
        dataVector.setValueCount(2);
        listVector.setValueCount(1);

        arrowStreamWriter.writeBatch();
        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"INT\",\"name\":\"id\",\"comment\":\"\"}," +
                "{\"type\":\"ARRAY\",\"name\":\"arr_int\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);
        // Test with type inference enabled
        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false, true);

        // Verify that type inference works when enabled
        Map<Integer, String> inferredTypes = rowBatch.getInferredArrayElementTypes();
        Assert.assertNotNull(inferredTypes);
        Assert.assertTrue(inferredTypes.containsKey(1)); // Column index 1 is arr_int
        String inferredType = inferredTypes.get(1);
        Assert.assertNotNull(inferredType);
        // Should infer "INT" as element type
        Assert.assertTrue("Expected inferred type to contain INT, but got: " + inferredType,
                inferredType.contains("INT") || inferredType.equals("INT"));
        
        // Verify data is correctly read
        Assert.assertTrue(rowBatch.hasNext());
        List<Object> row = rowBatch.next();
        Assert.assertEquals(1, row.get(0));
        
        @SuppressWarnings("unchecked")
        List<Object> arr = (List<Object>) row.get(1);
        Assert.assertNotNull(arr);
        Assert.assertEquals(2, arr.size());
        Assert.assertEquals(100, arr.get(0));
        Assert.assertEquals(200, arr.get(1));

        Assert.assertFalse(rowBatch.hasNext());
        
        root.close();
    }

    @Test
    public void testArrayTypeInferenceNested() throws DorisException, IOException {
        // Test nested array type inference
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        
        Field innerElementField = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field innerArrayField = new Field("element", FieldType.nullable(ArrowType.List.INSTANCE), 
                ImmutableList.of(innerElementField));
        Field outerArrayField = new Field("arr_nested", FieldType.nullable(ArrowType.List.INSTANCE), 
                ImmutableList.of(innerArrayField));
        
        childrenBuilder.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(outerArrayField);

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        IntVector idVector = (IntVector) root.getVector("id");
        idVector.setInitialCapacity(1);
        idVector.allocateNew();
        idVector.setSafe(0, 1);
        idVector.setValueCount(1);

        ListVector outerListVector = (ListVector) root.getVector("arr_nested");
        outerListVector.setInitialCapacity(1);
        outerListVector.allocateNew();
        
        outerListVector.startNewValue(0);
        ListVector innerListVector = (ListVector) outerListVector.getDataVector();
        innerListVector.startNewValue(0);
        IntVector dataVector = (IntVector) innerListVector.getDataVector();
        dataVector.setSafe(0, 1);
        innerListVector.endValue(0, 1);
        outerListVector.endValue(0, 1);
        
        dataVector.setValueCount(1);
        innerListVector.setValueCount(1);
        outerListVector.setValueCount(1);

        arrowStreamWriter.writeBatch();
        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[" +
                "{\"type\":\"INT\",\"name\":\"id\",\"comment\":\"\"}," +
                "{\"type\":\"ARRAY\",\"name\":\"arr_nested\",\"comment\":\"\"}" +
                "], \"status\":200}";

        Schema schema = MAPPER.readValue(schemaStr, Schema.class);
        // Test with type inference enabled
        RowBatch rowBatch = new RowBatch(scanBatchResult, schema, false, true);

        // Verify nested array type inference
        Map<Integer, String> inferredTypes = rowBatch.getInferredArrayElementTypes();
        Assert.assertNotNull(inferredTypes);
        Assert.assertTrue(inferredTypes.containsKey(1));
        String inferredType = inferredTypes.get(1);
        Assert.assertNotNull(inferredType);
        // Should infer nested array type like "ARRAY<ARRAY<INT>>" or "ARRAY<INT>"
        Assert.assertTrue("Expected inferred type to contain ARRAY, but got: " + inferredType,
                inferredType.contains("ARRAY"));

        root.close();
    }

}