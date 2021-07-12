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

import java.net.URI
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
import org.apache.spark.sql.extension.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.Aggregation
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/** Creates a factory for creating HdfsPartitionReader objects,
 *  for parquet files to be read using ColumnarBatches.
 *
 * @param pushdown has all pushdown options.
 * @param options the options including "path"
 * @param sharedConf - Hadoop configuration
 * @param sqlConf - SQL configuration.
 */
class HdfsColumnarPartitionReaderFactory(pushdown: Pushdown,
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

  private def buildReader(partition: HdfsPartition): VectorizedParquetRecordReader = {
    val conf = sharedConf.value.value
    val filePath = new Path(new URI(partition.name))
    lazy val footerFileMetaData =
      ParquetFileReader.readFooter(conf, filePath, SKIP_ROW_GROUPS).getFileMetaData
    val pushed = if (enableParquetFilterPushDown) {
      val parquetSchema = footerFileMetaData.getSchema
      // logger.info("parquet file schema: " + parquetSchema.toString)
      val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
        pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
      pushdown.filters
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        .flatMap(parquetFilters.createFilter)
        .reduceOption(FilterApi.and)
    } else {
      None
    }
    // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
    // *only* if the file was created by something other than "parquet-mr", so check the actual
    // writer here for this file.  We have to do this per-file, as each file in the table may
    // have different writers.
    // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
    def isCreatedByParquetMr: Boolean =
      footerFileMetaData.getCreatedBy().startsWith("parquet-mr")
    val convertTz = if (timestampConversion && !isCreatedByParquetMr) {
        Some(DateTimeUtils.getZoneId(conf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
      } else {
        None
      }
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
    val int96RebaseMode = DataSourceUtils.int96RebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      int96RebaseModeInRead)
    val reader = createParquetVectorizedReader(hadoopAttemptContext,
                                               pushed,
                                               convertTz,
                                               datetimeRebaseMode,
                                               int96RebaseMode)
    if (options.get("path").contains("ndphdfs") &&
        pushdown.isPushdownNeeded) {
      // In this case we need to use an InputFile based API,
      // since we need to provide our own stream.
      // When pushdown is not needed, we use the
      // standard HadoopInputFile since we do not need to pushdown
      // through our custom hdfs API.
      var store: HdfsStore = HdfsStoreFactory.getStore(pushdown, options,
                                                       sparkSession, sharedConf.value.value)
      val inputFile = HdfsNdpInputFile.fromPath(store, partition)
      reader.initialize(inputFile.asInstanceOf[InputFile], hadoopAttemptContext)
    } else {
      // For this case we use a split (one split per partition)
      val filePath = new Path(new URI(partition.name))
      val split = new FileSplit(filePath, partition.offset, partition.length, Array.empty[String])
      // val inputFile = HadoopInputFile.fromPath(new Path(file.filePath), conf)
      reader.initialize(split, hadoopAttemptContext)
    }
    reader
  }
  private def createParquetVectorizedReader(
      hadoopAttemptContext: TaskAttemptContextImpl,
      pushed: Option[FilterPredicate],
      convertTz: Option[ZoneId],
      datetimeRebaseMode: LegacyBehaviorPolicy.Value,
      int96RebaseMode: LegacyBehaviorPolicy.Value): VectorizedParquetRecordReader = {
    val taskContext = Option(TaskContext.get())
    val vectorizedReader = new VectorizedParquetRecordReader(
      convertTz.orNull,
      datetimeRebaseMode.toString,
      int96RebaseMode.toString,
      enableOffHeapColumnVector && taskContext.isDefined,
      batchSize)
    val iter = new RecordReaderIterator(vectorizedReader)
    // SPARK-23457 Register a task completion listener before `initialization`.
    taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
    vectorizedReader
  }
  private def createVectorizedReader(partition: HdfsPartition): VectorizedParquetRecordReader = {
    val vectorizedReader = buildReader(partition)
      .asInstanceOf[VectorizedParquetRecordReader]
    vectorizedReader.initBatch(new StructType(Array.empty[StructField]), InternalRow.empty)
    vectorizedReader
  }
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val part = partition.asInstanceOf[HdfsPartition]
    val vectorizedReader = createVectorizedReader(part)
    vectorizedReader.enableReturningBatches()
    new HdfsColumnarPartitionReader(vectorizedReader)
    // This aternate factory below is identical to the above, but
    // provides more verbose progress tracking.
    // new HdfsColumnarPartitionReaderProgress(vectorizedReader, batchSize, part)
  }
}

/** PartitionReader which returns a ColumnarBatch, and
 *  is relying on the Vectorized ParquetRecordReader to
 *  fetch the batches.
 *
 * @param vectorizedReader - Already initialized vectorizedReader
 *                           which provides the data for the PartitionReader.
 */
class HdfsColumnarPartitionReader(vectorizedReader: VectorizedParquetRecordReader)
  extends PartitionReader[ColumnarBatch] {
  override def next(): Boolean = {
    vectorizedReader.nextKeyValue()
  }
  override def get(): ColumnarBatch = {
    val batch = vectorizedReader.getCurrentValue.asInstanceOf[ColumnarBatch]
    batch
  }
  override def close(): Unit = vectorizedReader.close()
}
class HdfsColumnarPartitionReaderProgress(vectorizedReader: VectorizedParquetRecordReader,
                                          batchSize: Long, part: HdfsPartition)
  extends PartitionReader[ColumnarBatch] {
  private val logger = LoggerFactory.getLogger(getClass)
  private var index: Long = 0
  override def next(): Boolean = {
    logger.info(s"next batch partition: ${part.name} offset: ${part.offset} index: ${index}" +
                s"rows: ${vectorizedReader.totalRowCount()}")
    vectorizedReader.nextKeyValue()
  }
   private val logSize = (500000 / batchSize) * batchSize
  override def get(): ColumnarBatch = {
    val batch = vectorizedReader.getCurrentValue.asInstanceOf[ColumnarBatch]
    if ((index % logSize) == 0 ||
        (index + batchSize) >= vectorizedReader.totalRowCount()) {
      logger.info(s"batch rows: ${vectorizedReader.totalRowCount()} index: ${index} " +
                  s"offset: ${part.offset} partition: ${part.name}")
    }
    index += batchSize
    batch
  }
  override def close(): Unit = vectorizedReader.close()
}

/** Related routines for HdfsColumnarReaderFactory.
 */
object HdfsColumnarReaderFactory {

  def getHadoopConf(sparkSession: SparkSession, schema: StructType):
    Broadcast[org.apache.spark.util.SerializableConfiguration] = {
    val conf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(
      sparkSession.sessionState.newHadoopConf()))
    /* This is the same set of config options setup inside of
     * ParquetScan's createReaderFactory.
     */
    conf.value.value.set(ParquetInputFormat.READ_SUPPORT_CLASS,
                          classOf[ParquetReadSupport].getName)
    val schemaAsJson = schema.json
    conf.value.value.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      schemaAsJson)
    conf.value.value.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      schemaAsJson)
    conf.value.value.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    conf.value.value.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)
    conf.value.value.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    conf.value.value.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    conf.value.value.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)
    /* Our own config tweaks.
     */
    // conf.value.value.set("io.file.buffer.size", s"${1024 * 1024}")
    conf
  }
}
