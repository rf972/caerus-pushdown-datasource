/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasource.hdfs

import java.io.DataInputStream
import java.io.EOFException
import java.io.File
import java.io.FileWriter
import java.io.InputStream
import java.net.URI
import java.nio.ByteBuffer
import java.time.ZoneId
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

import com.github.datasource.common.Pushdown
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.io.InputFile
import org.dike.hdfs.NdpHdfsFileSystem
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.extension.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.Aggregation
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/** Creates a factory for creating HdfsPartitionReader objects,
 *  for parquet files to be read using ColumnarBatches.
 *
 * @param pushdown has all pushdown options.
 * @param options the options including "path"
 * @param sharedConf - Hadoop configuration
 * @param sqlConf - SQL configuration.
 */
class HdfsBinColPartitionReaderFactory(pushdown: Pushdown,
                                       options: util.Map[String, String],
 sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration],
 sqlConf: SQLConf)
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
  private val enableVectorizedReader: Boolean = sqlConf.parquetVectorizedReaderEnabled
  private val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
  private val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
  private val batchSize = sqlConf.parquetVectorizedReaderBatchSize
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val datetimeRebaseModeInRead = LegacyBehaviorPolicy.EXCEPTION.toString
  private val int96RebaseModeInRead = LegacyBehaviorPolicy.EXCEPTION.toString

  private val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("ndp")
      .getOrCreate()

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException("Cannot create row reader.");
  }
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val part = partition.asInstanceOf[HdfsPartition]
  // if (true) { // @TODO, check for if any rows.
  // logger.info("Using binary columnar partition reader " +
  //            part.name + " offset: " + part.offset)

  var store: HdfsStore = HdfsStoreFactory.getStore(pushdown, options,
                                                   sparkSession, sharedConf.value.value)
  val reader = new HdfsBinColVectReader(pushdown.readSchema, 4 * 1024,
                                        part,
                                        store.getStream(part).asInstanceOf[DataInputStream])
  logger.info("HdfsBinColVectReader created row group " + part.index)
  new HdfsBinColPartitionReader(reader, batchSize)
      // This alternate factory below is identical to the above, but
      // provides more verbose progress tracking.
      // new HdfsColumnarPartitionReaderProgress(vectorizedReader, batchSize, part)
    /* } else {
      /* If the row count is zero, it means that no result was
       * returned from ndp.  In this case, just use an
       * empty column reader so as not to give
       * an empty file to parquet-mr, which will result in error
       * due to an invalid parquet file (no footer).
       */
      logger.info("Using empty columnar partition reader " +
                  part.name + " offset: " + part.offset)
      new HdfsEmptyColumnarPartitionReader(vectorizedReader)
    } */
  }
}

/** PartitionReader which returns a ColumnarBatch, and
 *  is relying on the Vectorized ParquetRecordReader to
 *  fetch the batches.
 *
 * @param vectorizedReader - Already initialized vectorizedReader
 *                           which provides the data for the PartitionReader.
 */
class HdfsBinColPartitionReader(vectorizedReader: HdfsBinColVectReader,
                                batchSize: Integer)
  extends PartitionReader[ColumnarBatch] {
  private val logger = LoggerFactory.getLogger(getClass)
  override def next(): Boolean = {
    // logger.info("ColumnarPartitionReader next()")
    vectorizedReader.next()
  }
  private var totalRows: Long = 0L
  override def get(): ColumnarBatch = {
    val batch = vectorizedReader.get.asInstanceOf[ColumnarBatch]
    batch
  }
  override def close(): Unit = vectorizedReader.close()
}

/** Type of object encoded in binary.
 *  This follows the encoding values used by the NDP server.
 */
object NdpDataType extends Enumeration {
  type NdpDataType = Value
  val LongType = Value(1)
  val DoubleType = Value(2)
  val StringType = Value(3)
  val CharType = Value(4)
}
class HdfsBinColVectReader(schema: StructType,
                           batchSize: Integer,
                           part: HdfsPartition,
                           stream: DataInputStream) {
  private val logger = LoggerFactory.getLogger(getClass)
  // val columnVectors =
  //  OnHeapColumnVector.allocateColumns(batchSize, schema).asInstanceOf[
  //                                           Array[WritableColumnVector]]
  def next(): Boolean = {
    nextBatch()
  }
  def get(): ColumnarBatch = {
    columnarBatch
  }
  def close(): Unit = {
  }
  private var rowsReturned: Long = 0
  private var currentBatchSize: Int = 0
  private var batchIdx: Long = 0
  private val (numCols: Integer, dataTypes: Array[Int]) = {
    /* The NDP server encodes the number of columns followed by
     * the the type of each column.  All values are doubles.
     */
    try {
      val nColsLong = stream.readLong()
      val nCols: Integer = nColsLong.toInt
      // logger.info("nCols : " + String.valueOf(nCols))
      val dataTypes = new Array[Int](nCols)
      for (i <- 0 to nCols - 1) {
        dataTypes(i) = (stream.readLong()).toInt
        // logger.info(String.valueOf(i) + " : " + String.valueOf(dataTypes(i)))
      }
      // scalastyle:on println
      (nCols, dataTypes)
    } catch {
        case ex: Exception =>
        /* We do not expect to hit end of row, but if we do, it might mean that
         * the NDP query had nothing to return.
         */
          logger.info("Init Exception: " + ex)
          (0, new Array[Int](0))
        case ex: Throwable =>
          logger.info("Init Throwable: " + ex)
          (0, new Array[Int](0))
    }
  }
  val ndpColVectors = NdpColumnVector(batchSize, dataTypes, schema)
  val columnarBatch = new ColumnarBatch(ndpColVectors.asInstanceOf[Array[ColumnVector]])
  private var EOFReached = false
  def readNextRow(batchRow: Integer): Unit = {
    try {
      for (i <- 0 to numCols - 1) {
        NdpDataType(dataTypes(i)) match {
          case NdpDataType.LongType =>
            val intData = stream.readLong()
            // columnVectors(i).putLong(batchRow, intData)
          case NdpDataType.DoubleType =>
            val doubleData = stream.readDouble()
            // columnVectors(i).putDouble(batchRow, doubleData)
          case NdpDataType.StringType =>
            val strData = stream.readUTF()
            val bytes = UTF8String.fromString(strData).getBytes
            // columnVectors(i).putByteArray(batchRow, bytes, 0, bytes.length)
        }
      }
    } catch {
      case ex: EOFException =>
        EOFReached = true
        // logger.warn(ex.toString)
      case ex: Exception =>
        logger.warn(ex.toString)
    }
  }
  def readNextBatch(): Integer = {
    var rows: Integer = 0
    for (i <- 0 until numCols) {
      val currentRows = ndpColVectors(i).readColumn(stream)
      if (rows == 0) {
        rows = currentRows
      } else if (rows != 0 && currentRows != rows) {
        // We expect all rows in the batch to be the same size.
        throw new Exception("mismatch in rows ${currentRows} != ${rows}")
      }
    }
    rows
  }
  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  def nextBatch(): Boolean = {
    columnarBatch.setNumRows(0)
    // if (EOFReached) return false
    // if (rowsReturned == 0) {
      // logger.info(s"nextBatch total: ${rowsReturned}")
    // }
    val rows = readNextBatch()
    if (rows == 0) {
      // HdfsStore.logEnd(part.index)
      // logger.info(s"nextBatch Done rows: ${rows} total: ${rowsReturned}")
    }
    rowsReturned += rows
    columnarBatch.setNumRows(rows.toInt)
    currentBatchSize = rows
    batchIdx = 0
    if (rows > 0) { true } else { false }
  }
}

class NdpColumnVector(batchSize: Integer, dataType: Int, schema: StructType)
    extends ColumnVector(schema: StructType) {
  private val logger = LoggerFactory.getLogger(getClass)
  val (byteBuffer: ByteBuffer,
       stringIndex: Array[Int],
       stringLen: ByteBuffer,
       bufferLength: Integer) = {
    var stringIndex = Array[Int](0)
    var stringLen = ByteBuffer.allocate(0)
    val bytes: Int = NdpDataType(dataType) match {
      case NdpDataType.LongType => 8
      case NdpDataType.DoubleType => 8
      case NdpDataType.StringType =>
      // Assumes single byte length field.
      stringIndex = new Array[Int](batchSize)
      stringLen = ByteBuffer.allocate(batchSize)
      128
      case _ => 0
    }
    (ByteBuffer.allocate(batchSize * bytes),
     stringIndex, stringLen,
     (batchSize * bytes).asInstanceOf[Integer])
  }
  def close(): Unit = {}
  def getArray(row: Int): org.apache.spark.sql.vectorized.ColumnarArray = { null }
  def getBinary(row: Int): Array[Byte] = { null }
  def getBoolean(row: Int): Boolean = { false }
  def getByte(row: Int): Byte = { byteBuffer.get(row) }
  def getChild(row: Int): org.apache.spark.sql.vectorized.ColumnVector = { null }
  def getDecimal(row: Int, r: Int, p: Int): org.apache.spark.sql.types.Decimal = { Decimal(0) }
  def getDouble(row: Int): Double = { byteBuffer.getDouble(row * 8) }
  def getFloat(row: Int): Float = { byteBuffer.getFloat(row * 8) }
  def getInt(row: Int): Int = { byteBuffer.getInt(row * 4) }
  def getLong(row: Int): Long = { byteBuffer.getLong(row * 8) }
  def getMap(row: Int): org.apache.spark.sql.vectorized.ColumnarMap = { null }
  def getShort(row: Int): Short = { byteBuffer.getShort(row * 2) }
  def getUTF8String(row: Int): org.apache.spark.unsafe.types.UTF8String = {
    val offset = stringIndex(row)
    val length = stringLen.get(row)
    UTF8String.fromBytes(byteBuffer.array(), offset, length)
  }
  def hasNull(): Boolean = { false }
  def isNullAt(row: Int): Boolean = { false }
  def numNulls(): Int = { 0 }
  def readColumn(stream: DataInputStream): Int = {
    var rows: Int = 0
    try {
      var bytesRead = 0
      val numBytes = stream.readLong()
      rows = numBytes.toInt / 8
      NdpDataType(dataType) match {
        case NdpDataType.LongType =>
          if (rows > batchSize) {
            logger.warn(s"rows ${rows} > batchSize ${batchSize}")
          }
          stream.readFully(byteBuffer.array(), 0, rows * 8)
        case NdpDataType.DoubleType =>
          if (rows > batchSize) {
            logger.warn(s"rows ${rows} > batchSize ${batchSize}")
          }
          stream.readFully(byteBuffer.array(), 0, rows * 8)
        case NdpDataType.StringType =>
          rows = numBytes.toInt
          if (rows > batchSize) {
            logger.warn(s"rows ${rows} > batchSize ${batchSize}")
          }
          stream.readFully(stringLen.array(), 0, rows)
          var idx = 0
          for (i <- 0 until rows) {
            stringIndex(i) = idx
            idx += stringLen.get(i) & 0xFF
          }
          val textBytes = stream.readLong()
          if (textBytes > bufferLength) {
            logger.warn(s"textBytes ${textBytes} > bufferLength ${bufferLength}")
          }
          stream.readFully(byteBuffer.array(), 0, textBytes.toInt)
      }
    } catch {
      case ex: EOFException =>
        // logger.warn(ex.toString)
      case ex: Exception =>
        logger.warn(ex.toString)
        throw ex
    }
    rows
  }
}

object NdpColumnVector {

  def apply(batchSize: Integer,
            dataTypes: Array[Int],
            schema: StructType): Array[NdpColumnVector] = {
    var vectors = new Array[NdpColumnVector](dataTypes.length)
    for (i <- 0 until dataTypes.length) {
      vectors(i) = new NdpColumnVector(batchSize, dataTypes(i), schema);
    }
    vectors
  }
}
