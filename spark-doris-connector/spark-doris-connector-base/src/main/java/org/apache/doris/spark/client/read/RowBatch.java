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
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.util.IPUtils;

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseIntVector;
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
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.types.Decimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * row batch data container.
 */
public class RowBatch implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RowBatch.class);
    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();
    
    // JSON converter for ARRAY type serialization to maintain consistency with MAP/STRUCT/JSON types
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .toFormatter();

    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private final DateTimeFormatter dateTimeFormatter =
            DateTimeFormatter.ofPattern(DATETIME_PATTERN);
    private final DateTimeFormatter dateTimeV2Formatter =
            DateTimeFormatter.ofPattern(DATETIMEV2_PATTERN);
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final List<Row> rowBatch = new ArrayList<>();
    private final ArrowReader arrowReader;
    private final Schema schema;
    private RootAllocator rootAllocator;
    // offset for iterate the rowBatch
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<FieldVector> fieldVectors;
    
    // Cache for inferred array element types: column index -> inferred element type string
    private Map<Integer, String> inferredArrayElementTypes = null;
    
    // Performance optimization: Cache Arrow element types for ARRAY columns
    // Used to determine if JSON serialization is needed; primitive types need not be converted
    private Map<Integer, MinorType> arrayElementTypes = new HashMap<>();
    
    /**
     * Get inferred array element types
     * This can be used to update Schema with precise element types
     * 
     * @return Map of column index to inferred element type string, or null if not yet inferred
     */
    public Map<Integer, String> getInferredArrayElementTypes() {
        return inferredArrayElementTypes;
    }

    private final Boolean datetimeJava8ApiEnabled;
    private final Boolean enableArrayTypeInference;

    public RowBatch(TScanBatchResult nextResult, Schema schema, Boolean datetimeJava8ApiEnabled) throws DorisException {
        this(nextResult, schema, datetimeJava8ApiEnabled, false);
    }

    public RowBatch(TScanBatchResult nextResult, Schema schema, Boolean datetimeJava8ApiEnabled, Boolean enableArrayTypeInference) throws DorisException {

        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowReader = new ArrowStreamReader(new ByteArrayInputStream(nextResult.getRows()), rootAllocator);
        this.schema = schema;
        this.datetimeJava8ApiEnabled = datetimeJava8ApiEnabled;
        this.enableArrayTypeInference = enableArrayTypeInference;

        try {
            VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
            while (arrowReader.loadNextBatch()) {
                readBatch(root);
            }
        } catch (Exception e) {
            logger.error("Read Doris Data failed because: ", e);
            throw new DorisException(e.getMessage());
        } finally {
            close();
        }

    }

    public RowBatch(ArrowReader reader, Schema schema, Boolean datetimeJava8ApiEnabled) throws DorisException {
        this(reader, schema, datetimeJava8ApiEnabled, false);
    }

    public RowBatch(ArrowReader reader, Schema schema, Boolean datetimeJava8ApiEnabled, Boolean enableArrayTypeInference) throws DorisException {

        this.arrowReader = reader;
        this.schema = schema;
        this.datetimeJava8ApiEnabled = datetimeJava8ApiEnabled;
        this.enableArrayTypeInference = enableArrayTypeInference;

        try {
            VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
            readBatch(root);
        } catch (Exception e) {
            logger.error("Read Doris Data failed because: ", e);
            throw new DorisException(e.getMessage());
        }

    }

    private void readBatch(VectorSchemaRoot root) throws DorisException {
        fieldVectors = root.getFieldVectors();
        if (fieldVectors.size() > schema.size()) {
            logger.error("Data schema size '{}' should not be bigger than arrow field size '{}'.",
                    schema.size(), fieldVectors.size());
            throw new DorisException("Load Doris data failed, schema size of fetch data is wrong.");
        }
        if (fieldVectors.isEmpty() || root.getRowCount() == 0) {
            logger.debug("One batch in arrow has no data.");
            return;
        }
        
        // Infer array element types from Arrow Field on first batch (only if enabled)
        if (enableArrayTypeInference && inferredArrayElementTypes == null) {
            inferredArrayElementTypes = inferArrayElementTypes(root);
        }
        
        // Performance optimization: Cache Arrow element types for ARRAY columns
        // Used to determine if JSON serialization is needed; avoid unnecessary conversion of primitive types
        arrayElementTypes.clear();
        for (int col = 0; col < fieldVectors.size(); col++) {
            if (col < schema.size() && "ARRAY".equals(schema.get(col).getType())) {
                FieldVector vector = fieldVectors.get(col);
                if (vector instanceof ListVector) {
                    ListVector listVector = (ListVector) vector;
                    MinorType elementType = listVector.getDataVector().getMinorType();
                    arrayElementTypes.put(col, elementType);
                }
            }
        }
        
        rowCountInOneBatch = root.getRowCount();
        // init the rowBatch
        for (int i = 0; i < rowCountInOneBatch; ++i) {
            rowBatch.add(new Row(fieldVectors.size()));
        }
        convertArrowToRowBatch();
        readRowCount += root.getRowCount();
    }
    
    /**
     * Infer array element types from Arrow Schema
     * This provides precise type information instead of defaulting to StringType
     * 
     * @param root the VectorSchemaRoot containing Arrow Field information
     * @return Map of column index to inferred element type string (e.g., "INT", "STRING", "ARRAY")
     */
    private Map<Integer, String> inferArrayElementTypes(VectorSchemaRoot root) {
        Map<Integer, String> inferredTypes = new HashMap<>();
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = root.getSchema();
        
        for (int i = 0; i < fieldVectors.size() && i < schema.size(); i++) {
            org.apache.doris.spark.rest.models.Field dorisField = schema.get(i);
            if ("ARRAY".equals(dorisField.getType())) {
                Field arrowField = arrowSchema.getFields().get(i);
                String elementType = inferElementTypeFromArrowField(arrowField);
                if (elementType != null) {
                    inferredTypes.put(i, elementType);
                    logger.debug("Inferred ARRAY element type for column {}: {}", 
                            dorisField.getName(), elementType);
                }
            }
        }
        
        return inferredTypes;
    }
    
    /**
     * Recursively infer element type from Arrow Field
     * Returns a string representation of the Spark type (e.g., "INT", "STRING", "ARRAY<INT>")
     */
    private String inferElementTypeFromArrowField(Field field) {
        ArrowType arrowType = field.getType();
        
        // Check for List type (both INSTANCE and instanceof)
        if (arrowType == ArrowType.List.INSTANCE || arrowType instanceof ArrowType.List) {
            // Get element field from List
            List<Field> children = field.getChildren();
            if (children != null && !children.isEmpty()) {
                Field elementField = children.get(0);
                String elementType = inferElementTypeFromArrowField(elementField);
                if (elementType != null && !elementType.isEmpty()) {
                    return "ARRAY<" + elementType + ">";
                }
            }
            return "ARRAY<STRING>"; // Fallback
        } else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            if (intType.getIsSigned()) {
                if (intType.getBitWidth() == 8) return "TINYINT";
                if (intType.getBitWidth() == 16) return "SMALLINT";
                if (intType.getBitWidth() == 32) return "INT";
                if (intType.getBitWidth() == 64) return "BIGINT";
            }
            return "INT"; // Fallback
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
            if (floatType.getPrecision() == FloatingPointPrecision.SINGLE) {
                return "FLOAT";
            } else if (floatType.getPrecision() == FloatingPointPrecision.DOUBLE) {
                return "DOUBLE";
            }
            return "DOUBLE"; // Fallback
        } else if (arrowType == ArrowType.Utf8.INSTANCE) {
            return "STRING";
        } else if (arrowType == ArrowType.Binary.INSTANCE) {
            return "BINARY";
        } else if (arrowType instanceof ArrowType.Bool) {
            return "BOOLEAN";
        } else if (arrowType instanceof ArrowType.Decimal) {
            return "DECIMAL";
        } else if (arrowType instanceof ArrowType.Date) {
            ArrowType.Date dateType = (ArrowType.Date) arrowType;
            if (dateType.getUnit() == DateUnit.DAY) {
                return "DATE";
            }
            return "DATE"; // Fallback
        } else if (arrowType instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
            if (timestampType.getUnit() == TimeUnit.MICROSECOND) {
                return "DATETIME";
            }
            return "DATETIME"; // Fallback
        }
        
        return "STRING"; // Default fallback
    }

    public static LocalDateTime longToLocalDateTime(long time) {
        Instant instant;
        // Determine the timestamp accuracy and process it
        if (time < 10_000_000_000L) { // Second timestamp
            instant = Instant.ofEpochSecond(time);
        } else if (time < 10_000_000_000_000L) { // milli second
            instant = Instant.ofEpochMilli(time);
        } else { // micro second
            instant = Instant.ofEpochSecond(time / 1_000_000, (time % 1_000_000) * 1_000);
        }
        return LocalDateTime.ofInstant(instant, DEFAULT_ZONE_ID);
    }

    public boolean hasNext() {
        if (offsetInRowBatch >= readRowCount) {
            rowBatch.clear();
            return false;
        }
        return true;
    }

    private void addValueToRow(int rowIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " + rowCountInOneBatch;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        rowBatch.get(readRowCount + rowIndex).put(obj);
    }

    public void convertArrowToRowBatch() throws DorisException {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector curFieldVector = fieldVectors.get(col);
                MinorType mt = curFieldVector.getMinorType();

                final String colName = schema.get(col).getName();
                final String currentType = schema.get(col).getType();
                switch (currentType) {
                    case "NULL_TYPE":
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            addValueToRow(rowIndex, null);
                        }
                        break;
                    case "BOOLEAN":
                        Preconditions.checkArgument(mt.equals(MinorType.BIT),
                                typeMismatchMessage(colName, currentType, mt));
                        BitVector bitVector = (BitVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TINYINT":
                        Preconditions.checkArgument(mt.equals(MinorType.TINYINT),
                                typeMismatchMessage(colName, currentType, mt));
                        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "SMALLINT":
                        Preconditions.checkArgument(mt.equals(MinorType.SMALLINT),
                                typeMismatchMessage(colName, currentType, mt));
                        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "INT":
                        Preconditions.checkArgument(mt.equals(MinorType.INT),
                                typeMismatchMessage(colName, currentType, mt));
                        IntVector intVector = (IntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BIGINT":
                        Preconditions.checkArgument(mt.equals(MinorType.BIGINT),
                                typeMismatchMessage(colName, currentType, mt));
                        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "LARGEINT":
                        Preconditions.checkArgument(mt.equals(MinorType.FIXEDSIZEBINARY) ||
                                mt.equals(MinorType.VARCHAR), typeMismatchMessage(colName, currentType, mt));
                        if (mt.equals(MinorType.FIXEDSIZEBINARY)) {
                            FixedSizeBinaryVector largeIntVector = (FixedSizeBinaryVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (largeIntVector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                byte[] bytes = largeIntVector.get(rowIndex);
                                ArrayUtils.reverse(bytes);
                                BigInteger largeInt = new BigInteger(bytes);
                                addValueToRow(rowIndex, largeInt.toString());
                            }
                        } else {
                            VarCharVector largeIntVector = (VarCharVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (largeIntVector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                String stringValue = new String(largeIntVector.get(rowIndex));
                                addValueToRow(rowIndex, stringValue);
                            }
                        }
                        break;
                    case "IPV4":
                        Preconditions.checkArgument(mt.equals(MinorType.UINT4) || mt.equals(MinorType.INT) || mt.equals(MinorType.VARCHAR),
                                typeMismatchMessage(colName, currentType, mt));

                        if (mt.equals(MinorType.VARCHAR)) {
                            VarCharVector vector = (VarCharVector) curFieldVector;
                            for (int i = 0; i < rowCountInOneBatch; i++) {
                                addValueToRow(i, vector.isNull(i) ? null : new String(vector.get(i)));
                            }
                        } else {
                            BaseIntVector vector = (mt.equals(MinorType.INT)) ? (IntVector) curFieldVector : (UInt4Vector) curFieldVector;
                            for (int i = 0; i < rowCountInOneBatch; i++) {
                                addValueToRow(i, vector.isNull(i) ? null : IPUtils.convertLongToIPv4Address(vector.getValueAsLong(i)));
                            }
                        }
                        break;
                    case "FLOAT":
                        Preconditions.checkArgument(mt.equals(MinorType.FLOAT4),
                                typeMismatchMessage(colName, currentType, mt));
                        Float4Vector float4Vector = (Float4Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TIME":
                    case "DOUBLE":
                        Preconditions.checkArgument(mt.equals(MinorType.FLOAT8),
                                typeMismatchMessage(colName, currentType, mt));
                        Float8Vector float8Vector = (Float8Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BINARY":
                        Preconditions.checkArgument(mt.equals(MinorType.VARBINARY),
                                typeMismatchMessage(colName, currentType, mt));
                        VarBinaryVector varBinaryVector = (VarBinaryVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "DECIMAL":
                        Preconditions.checkArgument(mt.equals(MinorType.VARCHAR),
                                typeMismatchMessage(colName, currentType, mt));
                        VarCharVector varCharVectorForDecimal = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVectorForDecimal.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String decimalValue = new String(varCharVectorForDecimal.get(rowIndex));
                            Decimal decimal = new Decimal();
                            try {
                                decimal.set(new scala.math.BigDecimal(new BigDecimal(decimalValue)));
                            } catch (NumberFormatException e) {
                                String errMsg = "Decimal response result '" + decimalValue + "' is illegal.";
                                logger.error(errMsg, e);
                                throw new DorisException(errMsg);
                            }
                            addValueToRow(rowIndex, decimal);
                        }
                        break;
                    case "DECIMALV2":
                    case "DECIMAL32":
                    case "DECIMAL64":
                    case "DECIMAL128":
                    case "DECIMAL128I":
                        Preconditions.checkArgument(mt.equals(MinorType.DECIMAL),
                                typeMismatchMessage(colName, currentType, mt));
                        DecimalVector decimalVector = (DecimalVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (decimalVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            Decimal decimalV2 = Decimal.apply(decimalVector.getObject(rowIndex));
                            addValueToRow(rowIndex, decimalV2);
                        }
                        break;
                    case "DATE":
                    case "DATEV2":
                        Preconditions.checkArgument(mt.equals(MinorType.VARCHAR)
                                || mt.equals(MinorType.DATEDAY), typeMismatchMessage(colName, currentType, mt));
                        if (mt.equals(MinorType.VARCHAR)) {
                            VarCharVector date = (VarCharVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (date.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                String stringValue = new String(date.get(rowIndex));
                                LocalDate localDate = LocalDate.parse(stringValue);
                                if (datetimeJava8ApiEnabled) {
                                    addValueToRow(rowIndex, localDate);
                                } else {
                                    addValueToRow(rowIndex, Date.valueOf(localDate));
                                }
                            }
                        } else {
                            DateDayVector date = (DateDayVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (date.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                LocalDate localDate = LocalDate.ofEpochDay(date.get(rowIndex));
                                if (datetimeJava8ApiEnabled) {
                                    addValueToRow(rowIndex, localDate);
                                } else {
                                    addValueToRow(rowIndex, Date.valueOf(localDate));
                                }
                            }
                        }
                        break;
                    case "DATETIME":
                    case "DATETIMEV2":

                        if (mt.equals(MinorType.VARCHAR)) {
                            VarCharVector varCharVector = (VarCharVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (varCharVector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                String stringValue = completeMilliseconds(new String(varCharVector.get(rowIndex),
                                        StandardCharsets.UTF_8));
                                LocalDateTime dateTime = LocalDateTime.parse(stringValue, dateTimeV2Formatter);
                                if (datetimeJava8ApiEnabled) {
                                    Instant instant = dateTime.atZone(DEFAULT_ZONE_ID).toInstant();
                                    addValueToRow(rowIndex, instant);
                                } else {
                                    addValueToRow(rowIndex, Timestamp.valueOf(dateTime));
                                }
                            }
                        } else if (curFieldVector instanceof TimeStampVector) {
                            TimeStampVector timeStampVector = (TimeStampVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (timeStampVector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                LocalDateTime dateTime = getDateTime(rowIndex, timeStampVector);
                                if (datetimeJava8ApiEnabled) {
                                    Instant instant = dateTime.atZone(DEFAULT_ZONE_ID).toInstant();
                                    addValueToRow(rowIndex, instant);
                                } else {
                                    addValueToRow(rowIndex, Timestamp.valueOf(dateTime));
                                }
                            }
                        } else {
                            String errMsg = String.format("Unsupported type for DATETIMEV2, minorType %s, class is %s",
                                    mt.name(), curFieldVector.getClass());
                            throw new java.lang.IllegalArgumentException(errMsg);
                        }
                        break;
                    case "CHAR":
                    case "VARCHAR":
                    case "STRING":
                    case "JSONB":
                    case "VARIANT":
                        Preconditions.checkArgument(mt.equals(MinorType.VARCHAR),
                                typeMismatchMessage(colName, currentType, mt));
                        VarCharVector varCharVector = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String value = new String(varCharVector.get(rowIndex), StandardCharsets.UTF_8);
                            addValueToRow(rowIndex, value);
                        }
                        break;
                    case "IPV6":
                        Preconditions.checkArgument(mt.equals(MinorType.VARCHAR),
                                typeMismatchMessage(colName, currentType, mt));
                        VarCharVector ipv6VarcharVector = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (ipv6VarcharVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                break;
                            }
                            // Compatible with IPv6  in Doris 2.1.3 and above.
                            String ipv6Str = new String(ipv6VarcharVector.get(rowIndex));
                            if (ipv6Str.contains(":")){
                                addValueToRow(rowIndex, ipv6Str);
                            }else {
                                String ipv6Address = IPUtils.fromBigInteger(new BigInteger(ipv6Str));
                                addValueToRow(rowIndex, ipv6Address);
                            }

                        }
                        break;
                    case "ARRAY":
                        Preconditions.checkArgument(mt.equals(MinorType.LIST),
                                typeMismatchMessage(colName, currentType, mt));
                        ListVector listVector = (ListVector) curFieldVector;
                        UnionListReader listReader = listVector.getReader();
                        
                        // Performance optimization: Decide conversion strategy based on element type
                        MinorType elementType = arrayElementTypes.get(col);
                        boolean needsJson = needsJsonSerialization(elementType);
                        
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (listVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            // Position reader at current row and recursively convert ListVector
                            listReader.setPosition(rowIndex);
                            Object arrayValue = convertListVector(listVector, listReader);
                            
                            // ★ Optimization: Only convert types that need JSON serialization
                            // Primitive types keep List format for optimal performance
                            if (needsJson) {
                                // Complex types: Convert to JSON for consistency
                                String jsonString = convertArrayToJsonString(arrayValue);
                                addValueToRow(rowIndex, jsonString);
                            } else {
                                // Primitive types: Use List directly with zero overhead
                                addValueToRow(rowIndex, arrayValue);
                            }
                        }
                        break;
                    case "MAP":
                        Preconditions.checkArgument(mt.equals(MinorType.MAP),
                                typeMismatchMessage(colName, currentType, mt));
                        MapVector mapVector = (MapVector) curFieldVector;
                        UnionMapReader reader = mapVector.getReader();
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (mapVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            reader.setPosition(rowIndex);
                            Map<String, String> value = new HashMap<>();
                            while (reader.next()) {
                                value.put(Objects.toString(reader.key().readObject(), null),
                                        Objects.toString(reader.value().readObject(), null));
                            }
                            addValueToRow(rowIndex, value);
                        }
                        break;
                    case "STRUCT":
                        Preconditions.checkArgument(mt.equals(MinorType.STRUCT),
                                typeMismatchMessage(colName, currentType, mt));
                        StructVector structVector = (StructVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (structVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String value = structVector.getObject(rowIndex).toString();
                            addValueToRow(rowIndex, value);
                        }
                        break;
                    default:
                        String errMsg = "Unsupported type " + schema.get(col).getType() + "of col: " + schema.get(col).getName();
                        logger.error(errMsg);
                        throw new DorisException(errMsg);
                }
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public List<Object> next() {
        if (!hasNext()) {
            String errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        return rowBatch.get(offsetInRowBatch++).getCols();
    }

    private String typeMismatchMessage(final String columnName, final String sparkType, final MinorType arrowType) {
        final String messageTemplate = "Spark type for column %1$s is %2$s, but arrow type is %3$s.";
        return String.format(messageTemplate, columnName, sparkType, arrowType.name());
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowReader != null) {
                arrowReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }

    /**
     * Performance optimization: Determine if ARRAY element type needs JSON serialization
     * 
     * Primitive types do not need JSON conversion and can keep List format for optimal performance
     * Complex types need JSON conversion to maintain format consistency
     * 
     * Primitive types that do not need JSON:
     *   - BIT (boolean)
     *   - TINYINT/SMALLINT/INT/BIGINT (integer types)
     *   - FLOAT4/FLOAT8 (floating-point types)
     *   - DATEDAY (date)
     *   - TIMESTAMPMICRO/TIMESTAMPMICROTZ (timestamps)
     * 
     * Complex types that need JSON:
     *   - VARCHAR (strings - may contain special characters)
     *   - LIST (nested arrays)
     *   - STRUCT (structures)
     *   - MAP (mappings)
     *   - DECIMAL (decimal numbers)
     * 
     * @param elementType Arrow element type
     * @return true if JSON serialization is needed, false to keep List format
     */
    private boolean needsJsonSerialization(MinorType elementType) {
        if (elementType == null) {
            return false;  // If undetermined, keep List format (safe)
        }
        
        switch (elementType) {
            // Complex types: Need JSON serialization
            case VARCHAR:           // Strings (may contain special characters)
            case VARBINARY:         // Binary data
            case LIST:              // Nested arrays
            case STRUCT:            // Structures
            case MAP:               // Mappings
            case DECIMAL:           // Decimal numbers
                return true;
            
            // Primitive types: No JSON needed
            case BIT:               // Boolean
            case TINYINT:           // Byte
            case SMALLINT:          // Short integer
            case INT:               // Integer
            case BIGINT:            // Long integer
            case FLOAT4:            // Single-precision floating point
            case FLOAT8:            // Double-precision floating point
            case DATEDAY:           // Date
            case TIMESTAMPMICRO:    // Timestamp (microsecond precision)
            case TIMESTAMPMICROTZ:  // Timestamp (with timezone)
                return false;
            
            default:
                // Unknown types, use JSON conversion for safety
                return true;
        }
    }

    /**
     * Convert Java List object to JSON string for consistency with other complex types
     * This ensures ARRAY types are represented the same way as MAP, STRUCT, and JSON types
     * 
     * Examples:
     *   ["Alice", "Bob"] -> ["Alice","Bob"]
     *   [1, 2, 3] -> [1,2,3]
     *   [["a", "b"], ["c", "d"]] -> [["a","b"],["c","d"]]
     * 
     * @param arrayValue the Java List object from convertListVector
     * @return JSON string representation of the array
     */
    private String convertArrayToJsonString(Object arrayValue) {
        try {
            if (arrayValue == null) {
                return null;
            }
            if (!(arrayValue instanceof List)) {
                logger.warn("Expected List for ARRAY type, got {}", arrayValue.getClass().getName());
                return arrayValue.toString();
            }
            // Use Jackson to serialize the List to JSON string
            // This provides consistent, compact JSON formatting
            return objectMapper.writeValueAsString(arrayValue);
        } catch (Exception e) {
            logger.warn("Failed to convert array to JSON string: {}", e.getMessage());
            // Fallback: use toString() for the array
            return arrayValue.toString();
        }
    }

    /**
     * Recursively convert ListVector to native Java List
     * Supports nested arrays and all primitive types
     * 
     * Performance optimization: Pre-allocate list with estimated size to reduce reallocations
     * 
     * @param listVector the Arrow ListVector to convert
     * @param listReader the UnionListReader for the list vector (must be positioned)
     * @return Java List containing converted elements
     */
    private Object convertListVector(ListVector listVector, UnionListReader listReader) {
        // Performance optimization: estimate initial capacity to reduce reallocations
        // For nested arrays, we can't easily estimate, so use default initial capacity
        int estimatedSize = 16; // Default initial capacity for ArrayList
        List<Object> result = new ArrayList<>(estimatedSize);
        
        FieldVector elementVector = listVector.getDataVector();
        MinorType elementType = elementVector.getMinorType();
        
        while (listReader.next()) {
            Object element;
            
            // Handle nested arrays recursively
            if (elementType == MinorType.LIST) {
                ListVector nestedListVector = (ListVector) elementVector;
                org.apache.arrow.vector.complex.reader.FieldReader reader = listReader.reader();
                if (reader instanceof UnionListReader) {
                    // Recursively convert nested array
                    element = convertListVector(nestedListVector, (UnionListReader) reader);
                } else {
                    // Fallback: should not happen, but handle gracefully
                    logger.warn("Expected UnionListReader for nested array, got {}", reader.getClass().getName());
                    element = readPrimitiveValueFromReader(reader, elementType);
                }
            } else {
                // Handle primitive types
                element = readPrimitiveValueFromReader(listReader.reader(), elementType);
            }
            
            result.add(element);
        }
        
        return result;
    }
    
    /**
     * Read primitive value using FieldReader based on MinorType
     * Supports all Arrow primitive types for array elements
     * 
     * @param reader the FieldReader positioned at the element to read
     * @param type the Arrow MinorType of the element
     * @return the converted Java object, or null if not set
     */
    private Object readPrimitiveValueFromReader(org.apache.arrow.vector.complex.reader.FieldReader reader, MinorType type) {
        if (!reader.isSet()) {
            return null;
        }
        
        switch (type) {
            case BIT:
                return reader.readBoolean();
            case TINYINT:
                return reader.readByte();
            case SMALLINT:
                return reader.readShort();
            case INT:
                return reader.readInteger();
            case BIGINT:
                return reader.readLong();
            case FLOAT4:
                return reader.readFloat();
            case FLOAT8:
                return reader.readDouble();
            case VARCHAR:
                return reader.readText().toString();
            case VARBINARY:
                return reader.readByteArray();
            case DECIMAL:
                return reader.readBigDecimal();
            case DATEDAY:
                // DateDayVector stores days since epoch (1970-01-01), convert to LocalDate
                int days = reader.readInteger();
                return LocalDate.ofEpochDay(days);
            case TIMESTAMPMICRO:
            case TIMESTAMPMICROTZ:
                long time = reader.readLong();
                return longToLocalDateTime(time);
            default:
                // Fallback: try to read as generic object
                Object obj = reader.readObject();
                if (obj != null) {
                    return obj;
                }
                logger.warn("Unsupported element type in ARRAY: {}, returning null", type);
                return null;
        }
    }

    public LocalDateTime getDateTime(int rowIndex, FieldVector fieldVector) {
        TimeStampVector vector = (TimeStampVector) fieldVector;
        if (vector.isNull(rowIndex)) {
            return null;
        }
        // Note: Currently, the scale of doris's arrow datetimev2 is hardcoded to 6,
        // and there is also a time zone consideration in arrow conversion.
        // Using timestamp to convert first, which handles microsecond precision correctly.
        long time = vector.get(rowIndex);
        return longToLocalDateTime(time);
    }

    public static String completeMilliseconds(String stringValue) {
        if (stringValue.length() == DATETIMEV2_PATTERN.length()) {
            return stringValue;
        }

        if (stringValue.length() < DATETIME_PATTERN.length()) {
            return stringValue;
        }

        StringBuilder sb = new StringBuilder(stringValue);
        if (stringValue.length() == DATETIME_PATTERN.length()) {
            sb.append(".");
        }
        while (sb.toString().length() < DATETIMEV2_PATTERN.length()) {
            sb.append(0);
        }
        return sb.toString();
    }

    public static class Row {
        private final List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }
    }
}
