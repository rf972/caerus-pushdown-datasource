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

// scalastyle:off
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

import com.github.datasource.common.Pushdown
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.dike.hdfs.NdpHdfsFileSystem
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.Aggregation
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.time.ZoneId
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetRecordReader}
import org.apache.parquet.io.InputFile

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.extension.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.extension.parquet._
// scalastyle:on

/** Creates a factory for creating HdfsPartitionReader objects
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class HdfsColumnarPartitionReaderFactory(pushdown: Pushdown,
                                 options: util.Map[String, String],
 sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration])
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  private val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("ndp")
      .getOrCreate()
  logger.trace("Created")
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new HdfsPartitionReader(pushdown, options, partition.asInstanceOf[HdfsPartition],
                            sparkSession, sharedConf)
  }
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  private def buildReader(file: PartitionedFile): VectorizedParquetRecordReader = {
    val conf = sharedConf.value.value
    val filePath = new Path(new URI(file.filePath))
    lazy val footerFileMetaData =
      ParquetFileReader.readFooter(conf, filePath, SKIP_ROW_GROUPS).getFileMetaData
    // Try to push down filters when filter push-down is enabled.
    val pushed = None
    // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
    // *only* if the file was created by something other than "parquet-mr", so check the actual
    // writer here for this file.  We have to do this per-file, as each file in the table may
    // have different writers.
    // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
    def isCreatedByParquetMr: Boolean =
      footerFileMetaData.getCreatedBy().startsWith("parquet-mr")
    val convertTz = None /*
      if (timestampConversion && !isCreatedByParquetMr) {
        Some(DateTimeUtils.getZoneId(conf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
      } else {
        None
      } */
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      LegacyBehaviorPolicy.EXCEPTION.toString)
    val int96RebaseMode = DataSourceUtils.int96RebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      LegacyBehaviorPolicy.EXCEPTION.toString)
    val reader = createParquetVectorizedReader(hadoopAttemptContext,
                                               pushed,
                                               convertTz,
                                               datetimeRebaseMode,
                                               int96RebaseMode)
    // val inputFile = HadoopInputFile.fromPath(new Path(file.filePath), conf)
    val inputFile = NdpInputFile.fromPath(file.filePath, conf)
    reader.initialize(inputFile.asInstanceOf[InputFile], hadoopAttemptContext)
    reader
  }
  private def createParquetVectorizedReader(
      hadoopAttemptContext: TaskAttemptContextImpl,
      pushed: Option[FilterPredicate],
      convertTz: Option[ZoneId],
      datetimeRebaseMode: LegacyBehaviorPolicy.Value,
      int96RebaseMode: LegacyBehaviorPolicy.Value): VectorizedParquetRecordReader = {
    val taskContext = Option(TaskContext.get())
    /* We changed the below to be default values just like is done inside of
     * parquetPartitionReaderFactory.
     */
    val enableOffHeapColumnVector = false
    val capacity = 4*1024 // batch size
    val vectorizedReader = new VectorizedParquetRecordReader(
      convertTz.orNull,
      datetimeRebaseMode.toString,
      int96RebaseMode.toString,
      enableOffHeapColumnVector && taskContext.isDefined,
      capacity)
    val iter = new RecordReaderIterator(vectorizedReader)
    // SPARK-23457 Register a task completion listener before `initialization`.
    taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
    vectorizedReader
  }
  private def createVectorizedReader(file: PartitionedFile): VectorizedParquetRecordReader = {
    val vectorizedReader = buildReader(file)
      .asInstanceOf[VectorizedParquetRecordReader]
    vectorizedReader.initBatch(new StructType(Array.empty[StructField]), file.partitionValues)
    vectorizedReader
  }
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val part = partition.asInstanceOf[HdfsPartition]
    val file = PartitionedFile(InternalRow.empty, part.name,
                               0, part.length)
    val vectorizedReader = createVectorizedReader(file)
    vectorizedReader.enableReturningBatches()

    new PartitionReader[ColumnarBatch] {
      override def next(): Boolean = vectorizedReader.nextKeyValue()

      override def get(): ColumnarBatch =
        vectorizedReader.getCurrentValue.asInstanceOf[ColumnarBatch]

      override def close(): Unit = vectorizedReader.close()
    }
  }
}

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
    conf.value.value.set("io.file.buffer.size", s"${1024 * 1024}")
    conf.value.value.set("fs.ndphdfs.impl", classOf[org.dike.hdfs.NdpHdfsFileSystem].getName)
    conf
  }
}