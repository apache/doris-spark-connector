package org.apache.doris.spark.client.read

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader}
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.{BaseIntVector, BigIntVector, BitVector, DateDayVector, DecimalVector, FieldVector, FixedSizeBinaryVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampVector, TinyIntVector, UInt4Vector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.commons.lang3.ArrayUtils
import org.apache.doris.sdk.thrift.TScanBatchResult
import org.apache.doris.spark.client.DorisSchema
import org.apache.doris.spark.util.IPUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.Decimal

import java.io.{ByteArrayInputStream, IOException}
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.sql.Date
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util
import java.util.Objects
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RowBatch {
  private val DEFAULT_ZONE_ID = ZoneId.systemDefault
  private val DATE_TIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true).toFormatter
  private val DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss"
  private val DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS"

  def longToLocalDateTime(time: Long): LocalDateTime = {
    var instant: Instant = null
    // Determine the timestamp accuracy and process it
    if (time < 10000000000L) { // Second timestamp
      instant = Instant.ofEpochSecond(time)
    }
    else if (time < 10000000000000L) { // milli second
      instant = Instant.ofEpochMilli(time)
    }
    else { // micro second
      instant = Instant.ofEpochSecond(time / 1000000, (time % 1000000) * 1000)
    }
    LocalDateTime.ofInstant(instant, DEFAULT_ZONE_ID)
  }

  def completeMilliseconds(stringValue: String): String = {
    if (stringValue.length == DATETIMEV2_PATTERN.length) return stringValue
    if (stringValue.length < DATETIME_PATTERN.length) return stringValue
    val sb = new StringBuilder(stringValue)
    if (stringValue.length == DATETIME_PATTERN.length) sb.append(".")
    while (sb.toString.length < DATETIMEV2_PATTERN.length) sb.append(0)
    sb.toString
  }

  class Row private[client](colCount: Int) {

    private val cols: mutable.ListBuffer[Any] = ListBuffer[Any]()

    private[client] def getCols = cols

    def put(o: Any): Unit = {
      cols += o
    }
  }
}

class RowBatch extends Logging {
  final private var rows: mutable.Buffer[RowBatch.Row] = mutable.ListBuffer[RowBatch.Row]()
  final private var arrowReader: ArrowReader = _
  final private var schema: DorisSchema = _
  final private val dateTimeFormatter = DateTimeFormatter.ofPattern(RowBatch.DATETIME_PATTERN)
  final private val dateTimeV2Formatter = DateTimeFormatter.ofPattern(RowBatch.DATETIMEV2_PATTERN)
  final private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private var rootAllocator: RootAllocator = _
  // offset for iterate the rowBatch
  private var offsetInRowBatch = 0
  private var rowCountInOneBatch = 0
  private var readRowCount = 0
  private var fieldVectors: util.List[FieldVector] = null

  def this(nextResult: TScanBatchResult, schema: DorisSchema) {
    this()
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE)
    this.arrowReader = new ArrowStreamReader(new ByteArrayInputStream(nextResult.getRows), rootAllocator)
    this.schema = schema
    try {
      val root = arrowReader.getVectorSchemaRoot
      while (arrowReader.loadNextBatch) readBatch(root)
    } catch {
      case e: Exception =>
        log.error("Read Doris Data failed because: ", e)
        throw new Exception(e.getMessage)
    } finally close()
  }

  def this(reader: ArrowReader, schema: DorisSchema) {
    this()
    this.arrowReader = reader
    this.schema = schema
    try {
      val root = arrowReader.getVectorSchemaRoot
      readBatch(root)
    } catch {
      case e: Exception =>
        log.error("Read Doris Data failed because: ", e)
        throw new Exception(e.getMessage)
    }
  }

  @throws[Exception]
  private def readBatch(root: VectorSchemaRoot): Unit = {
    fieldVectors = root.getFieldVectors
    if (fieldVectors.size > schema.properties.size) {
      log.error("Data schema size '{}' should not be bigger than arrow field size '{}'.",
        schema.properties.size, fieldVectors.size)
      throw new Exception("Load Doris data failed, schema size of fetch data is wrong.")
    }
    if (fieldVectors.isEmpty || root.getRowCount == 0) {
      log.debug("One batch in arrow has no data.")
      return
    }
    rowCountInOneBatch = root.getRowCount
    // init the rowBatch
    for (i <- 0 until rowCountInOneBatch) {
      rows += new RowBatch.Row(fieldVectors.size)
    }
    convertArrowToRowBatch()
    readRowCount += root.getRowCount
  }

  def hasNext: Boolean = {
    if (offsetInRowBatch >= readRowCount) {
      rows.clear()
      return false
    }
    true
  }

  private def addValueToRow(rowIndex: Int, obj: Any): Unit = {
    if (rowIndex > rowCountInOneBatch) {
      val errMsg = "Get row offset: " + rowIndex + " larger than row size: " + rowCountInOneBatch
      log.error(errMsg)
      throw new NoSuchElementException(errMsg)
    }
    rows(readRowCount + rowIndex).put(obj)
  }

  @throws[Exception]
  def convertArrowToRowBatch(): Unit = {
    try for (col <- 0 until fieldVectors.size) {
      val curFieldVector = fieldVectors.get(col)
      val mt = curFieldVector.getMinorType
      val currentType = schema.properties(col).`type`
      currentType match {
        case "NULL_TYPE" =>
          for (rowIndex <- 0 until rowCountInOneBatch) {
            addValueToRow(rowIndex, null)
          }

        case "BOOLEAN" =>
          require(mt == Types.MinorType.BIT, typeMismatchMessage(currentType, mt))
          val bitVector = curFieldVector.asInstanceOf[BitVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[BitVector](bitVector, rowIndex, (vector, index) => vector.get(index) != 0)
            addValueToRow(rowIndex, fieldValue)
          }

        case "TINYINT" =>
          require(mt == Types.MinorType.TINYINT, typeMismatchMessage(currentType, mt))
          val tinyIntVector = curFieldVector.asInstanceOf[TinyIntVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[TinyIntVector](tinyIntVector, rowIndex, (vector, index) => vector.get(index))
            addValueToRow(rowIndex, fieldValue)
          }

        case "SMALLINT" =>
          require(mt == Types.MinorType.SMALLINT, typeMismatchMessage(currentType, mt))
          val smallIntVector = curFieldVector.asInstanceOf[SmallIntVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[SmallIntVector](smallIntVector, rowIndex, (vector, index) => vector.get(index))
            addValueToRow(rowIndex, fieldValue)
          }

        case "INT" =>
          require(mt == Types.MinorType.INT, typeMismatchMessage(currentType, mt))
          val intVector = curFieldVector.asInstanceOf[IntVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[IntVector](intVector, rowIndex, (vector, index) => vector.get(index))
            addValueToRow(rowIndex, fieldValue)
          }

        case "BIGINT" =>
          require(mt == Types.MinorType.BIGINT, typeMismatchMessage(currentType, mt))
          val bigIntVector = curFieldVector.asInstanceOf[BigIntVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[BigIntVector](bigIntVector, rowIndex, (vector, index) => vector.get(index))
            addValueToRow(rowIndex, fieldValue)
          }

        case "LARGEINT" =>
          require(mt == Types.MinorType.FIXEDSIZEBINARY || mt == Types.MinorType.VARCHAR, typeMismatchMessage(currentType, mt))
          if (mt == Types.MinorType.FIXEDSIZEBINARY) {
            val largeIntVector = curFieldVector.asInstanceOf[FixedSizeBinaryVector]
            for (rowIndex <- 0 until rowCountInOneBatch) {
              val fieldValue = nullOrVectorValue[FixedSizeBinaryVector](largeIntVector, rowIndex, (vector, index) => {
                val bytes = vector.get(index)
                ArrayUtils.reverse(bytes)
                val largeInt = new BigInteger(bytes)
                Decimal.apply(largeInt)
              })
              addValueToRow(rowIndex, fieldValue)
            }
          } else {
            val largeIntVector = curFieldVector.asInstanceOf[VarCharVector]
            for (rowIndex <- 0 until rowCountInOneBatch) {
              val fieldValue = nullOrVectorValue[VarCharVector](largeIntVector, rowIndex, (vector, index) => {
                val stringValue = new String(vector.get(index))
                val largeInt = new BigInteger(stringValue)
                Decimal.apply(largeInt)
              })
              addValueToRow(rowIndex, fieldValue)
            }
          }

        case "IPV4" =>
          require(mt == Types.MinorType.UINT4 || mt == Types.MinorType.INT, typeMismatchMessage(currentType, mt))
          var ipv4Vector: BaseIntVector = null
          if (mt == Types.MinorType.INT) ipv4Vector = curFieldVector.asInstanceOf[IntVector]
          else ipv4Vector = curFieldVector.asInstanceOf[UInt4Vector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[BaseIntVector](ipv4Vector, rowIndex, (vector, index) =>
              IPUtils.convertLongToIPv4Address(vector.getValueAsLong(index)))
            addValueToRow(rowIndex, fieldValue)
          }

        case "FLOAT" =>
          require(mt == Types.MinorType.FLOAT4, typeMismatchMessage(currentType, mt))
          val float4Vector = curFieldVector.asInstanceOf[Float4Vector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[Float4Vector](float4Vector, rowIndex, (vector, index) =>
              vector.get(index))
            addValueToRow(rowIndex, fieldValue)
          }

        case "TIME" =>
        case "DOUBLE" =>
          require(mt == Types.MinorType.FLOAT8, typeMismatchMessage(currentType, mt))
          val float8Vector = curFieldVector.asInstanceOf[Float8Vector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[Float8Vector](float8Vector, rowIndex, (vector, index) =>
              vector.get(index))
            addValueToRow(rowIndex, fieldValue)
          }

        case "BINARY" =>
          require(mt == Types.MinorType.VARBINARY, typeMismatchMessage(currentType, mt))
          val varBinaryVector = curFieldVector.asInstanceOf[VarBinaryVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[VarBinaryVector](varBinaryVector, rowIndex, (vector, index) => vector.get(index))
            addValueToRow(rowIndex, fieldValue)
          }

        case "DECIMAL" =>
          require(mt == Types.MinorType.VARCHAR, typeMismatchMessage(currentType, mt))
          val varCharVectorForDecimal = curFieldVector.asInstanceOf[VarCharVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[VarCharVector](varCharVectorForDecimal, rowIndex, (vector, index) => {
              val decimalValue = new String(vector.get(index))
              try Decimal(math.BigDecimal(decimalValue))
              catch {
                case e: NumberFormatException =>
                  val errMsg = "Decimal response result '" + decimalValue + "' is illegal."
                  log.error(errMsg, e)
                  throw new Exception(errMsg)
              }
            })
            addValueToRow(rowIndex, fieldValue)
          }

        case "DECIMALV2" | "DECIMAL32" | "DECIMAL64" | "DECIMAL128I" =>
          require(mt == Types.MinorType.DECIMAL, typeMismatchMessage(currentType, mt))
          val decimalVector = curFieldVector.asInstanceOf[DecimalVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[DecimalVector](decimalVector, rowIndex, (vector, index) => {
              Decimal(vector.getObject(index))
            })
            addValueToRow(rowIndex, fieldValue)
          }

        case "DATE" | "DATEV2" =>
          require(mt == Types.MinorType.VARCHAR || mt == Types.MinorType.DATEDAY, typeMismatchMessage(currentType, mt))
          if (mt == Types.MinorType.VARCHAR) {
            val date = curFieldVector.asInstanceOf[VarCharVector]
            for (rowIndex <- 0 until rowCountInOneBatch) {
              val fieldValue = nullOrVectorValue[VarCharVector](date, rowIndex, (vector, index) => {
                val stringValue = new String(vector.get(index))
                val localDate = LocalDate.parse(stringValue)
                Date.valueOf(localDate)
              })
              addValueToRow(rowIndex, fieldValue)
            }
          }
          else {
            val date = curFieldVector.asInstanceOf[DateDayVector]
            for (rowIndex <- 0 until rowCountInOneBatch) {
              val fieldValue = nullOrVectorValue[DateDayVector](date, rowIndex, (vector, index) => {
                val localDate = LocalDate.ofEpochDay(vector.get(index))
                Date.valueOf(localDate)
              })
              addValueToRow(rowIndex, fieldValue)
            }
          }

        case "DATETIME" | "DATETIMEV2" =>
          require(mt == Types.MinorType.TIMESTAMPMICRO || mt == MinorType.VARCHAR || mt == MinorType.TIMESTAMPMILLI
            || mt == MinorType.TIMESTAMPSEC, typeMismatchMessage(currentType, mt))
          typeMismatchMessage(currentType, mt)
          if (mt == Types.MinorType.VARCHAR) {
            val varCharVector = curFieldVector.asInstanceOf[VarCharVector]
            for (rowIndex <- 0 until rowCountInOneBatch) {
              val fieldValue = nullOrVectorValue[VarCharVector](varCharVector, rowIndex, (vector, index) =>
                new String(vector.get(index), StandardCharsets.UTF_8))
              addValueToRow(rowIndex, fieldValue)
            }
          }
          else curFieldVector match {
            case timeStampVector: TimeStampVector =>
              for (rowIndex <- 0 until rowCountInOneBatch) {
                val fieldValue = nullOrVectorValue[TimeStampVector](timeStampVector, rowIndex, (vector, index) => {
                  val dateTime = getDateTime(index, vector)
                  RowBatch.DATE_TIME_FORMATTER.format(dateTime)
                })
                addValueToRow(rowIndex, fieldValue)
              }
            case _ =>
          }
        case "CHAR" | "VARCHAR" | "STRING" | "JSONB" | "VARIANT" =>
          require(mt == Types.MinorType.VARCHAR, typeMismatchMessage(currentType, mt))
          val varCharVector = curFieldVector.asInstanceOf[VarCharVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[VarCharVector](varCharVector, rowIndex, (vector, index) =>
              new String(vector.get(index), StandardCharsets.UTF_8))
            addValueToRow(rowIndex, fieldValue)
          }

        case "IPV6" =>
          require(mt == Types.MinorType.VARCHAR, typeMismatchMessage(currentType, mt))
          val ipv6VarcharVector = curFieldVector.asInstanceOf[VarCharVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[VarCharVector](ipv6VarcharVector, rowIndex, (vector, index) => {
              val ipv6Str = new String(ipv6VarcharVector.get(rowIndex))
              IPUtils.fromBigInteger(new BigInteger(ipv6Str))
            })
            addValueToRow(rowIndex, fieldValue)
          }

        case "ARRAY" =>
          require(mt == Types.MinorType.LIST, typeMismatchMessage(currentType, mt))
          val listVector = curFieldVector.asInstanceOf[ListVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[ListVector](listVector, rowIndex, (vector, index) =>
              vector.getObject(index).toString)
            addValueToRow(rowIndex, fieldValue)
          }

        case "MAP" =>
          require(mt == Types.MinorType.MAP, typeMismatchMessage(currentType, mt))
          val mapVector = curFieldVector.asInstanceOf[MapVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[MapVector](mapVector, rowIndex, (vector, index) => {
              val reader = vector.getReader
              reader.setPosition(index)
              val value = new util.HashMap[String, String]
              while (reader.next) {
                value.put(Objects.toString(reader.key.readObject, null), Objects.toString(reader.value.readObject, null))
              }
              value.asScala
            })
            addValueToRow(rowIndex, fieldValue)
          }

        case "STRUCT" =>
          require(mt == Types.MinorType.STRUCT, typeMismatchMessage(currentType, mt))
          val structVector = curFieldVector.asInstanceOf[StructVector]
          for (rowIndex <- 0 until rowCountInOneBatch) {
            val fieldValue = nullOrVectorValue[StructVector](structVector, rowIndex, (vector, index) =>
              vector.getObject(index).toString)
            addValueToRow(rowIndex, fieldValue)
          }

        case _ =>
          val errMsg = "Unsupported type " + schema.properties(col).`type`
          log.error(errMsg)
          throw new Exception(errMsg)
      }
    }
    catch {
      case e: Exception =>
        close()
        throw e
    }
  }

  def nullOrVectorValue[V <: FieldVector](vector: V, index: Int, getter: (V, Int) => Any): Any = {
    if (vector.isNull(index)) null else getter.apply(vector, index)
  }

  def next: mutable.ListBuffer[Any] = {
    if (!hasNext) {
      val errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount
      log.error(errMsg)
      throw new NoSuchElementException(errMsg)
    }
    rows({
      offsetInRowBatch += 1;
      offsetInRowBatch - 1
    }).getCols
  }

  private def typeMismatchMessage(sparkType: String, arrowType: Types.MinorType) = {
    val messageTemplate = "Spark type is %1$s, but arrow type is %2$s."
    String.format(messageTemplate, sparkType, arrowType.name)
  }

  def getReadRowCount: Int = readRowCount

  def close(): Unit = {
    try {
      if (arrowReader != null) arrowReader.close()
      if (rootAllocator != null) rootAllocator.close()
    } catch {
      case ioe: IOException =>


      // do nothing
    }
  }

  def getDateTime(rowIndex: Int, fieldVector: FieldVector): LocalDateTime = {
    val vector = fieldVector.asInstanceOf[TimeStampVector]
    if (vector.isNull(rowIndex)) return null
    // todo: Currently, the scale of doris's arrow datetimev2 is hardcoded to 6,
    //  and there is also a time zone problem in arrow, so use timestamp to convert first
    val time = vector.get(rowIndex)
    RowBatch.longToLocalDateTime(time)
  }
}
