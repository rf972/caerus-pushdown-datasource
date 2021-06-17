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
// scalastyle:off
package com.github.datasource.hdfs

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

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration
// scalastyle:on

/** A scan object that works on HDFS files.
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class HdfsScan(schema: StructType,
               options: util.Map[String, String],
               filters: Array[Filter], prunedSchema: StructType,
               pushedAggregation: Aggregation)
      extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def readSchema(): StructType = prunedSchema

  private def init(): Unit = {
    if (options.get("format") == "parquet") {
      // The default for parquet is to use column names for now.
      options.put("useColumnNames", "")
    }
  }
  init()
  override def toBatch: Batch = this

  protected val pushdown = new Pushdown(schema, prunedSchema, filters,
                                        pushedAggregation, options)

  private val maxPartSize: Long = (1024 * 1024 * 128)
  private var partitions: Array[InputPartition] = getPartitions()

  private def createPartitions(blockMap: Map[String, Array[BlockLocation]],
                               store: HdfsStore): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    if (options.containsKey("partitions") &&
          options.get("partitions").toInt == 1) {
      // Generate one partition per file
      for ((fName, blockList) <- blockMap) {
        a += new HdfsPartition(index = 0, offset = 0, length = store.getLength(fName),
                               name = fName)
      }
    } else {
      // Generate one partition per file, per hdfs block
      for ((fName, blockList) <- blockMap) {
        // Generate one partition per hdfs block.
        for (block <- blockList) {
          a += new HdfsPartition(index = i, offset = block.getOffset, length = block.getLength,
                                 name = fName)
          i += 1
        }
      }
    }
    a.toArray
  }
  private def createPartitionsParquet(blockMap: Map[String, Array[BlockLocation]],
                                      store: HdfsStore): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    val conf = new Configuration()
    val readOptions = HadoopReadOptions.builder(conf)
                                       .build()
    logger.trace("blockMap: {}", blockMap.mkString(", "))
    // For now just one partition per file.
    if ( true || (options.containsKey("partitions") &&
                  options.get("partitions").toInt == 1)) {
      // Generate one partition per file
      for ((fName, blockList) <- blockMap) {
        val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(fName), conf),
                                            readOptions)
        val parquetBlocks = reader.getFooter.getBlocks
        val parquetBlock = parquetBlocks.get(0)
        a += new HdfsPartition(index = 0, offset = parquetBlock.getStartingPos,
                               length = store.getLength(fName), // parquetBlock.getTotalByteSize,
                               name = fName,
                               store.getModifiedTime(fName))
      }
    } else {
      // Generate one partition per file, per hdfs block
      for ((fName, blockList) <- blockMap) {
        val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(fName),
                                                                     conf), readOptions)
        val parquetBlocks = reader.getFooter.getBlocks
        // Generate one partition per row Group.
        for (i <- 0 to parquetBlocks.size - 1) {
          val parquetBlock = parquetBlocks.get(i)
          a += new HdfsPartition(index = i, offset = parquetBlock.getStartingPos,
                                 length = parquetBlock.getTotalByteSize,
                                 name = fName,
                                 store.getModifiedTime(fName))
        }
      }
    }
    logger.info(a.mkString(", "))
    a.toArray
  }
  private val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("ndp")
      .getOrCreate()
  val broadcastedHadoopConf = HdfsColumnarReaderFactory.getHadoopConf(sparkSession,
                                                                      pushdown.schema)
  /** Returns an Array of S3Partitions for a given input file.
   *  the file is selected by options("path").
   *  If there is one file, then we will generate multiple partitions
   *  on that file if large enough.
   *  Otherwise we generate one partition per file based partition.
   *
   * @return array of S3Partitions
   */
  private def getPartitions(): Array[InputPartition] = {
    var store: HdfsStore = HdfsStoreFactory.getStore(pushdown, options,
                                                     sparkSession, broadcastedHadoopConf)
    val fileName = store.filePath
    val blocks : Map[String, Array[BlockLocation]] = store.getBlockList(fileName)
    options.get("format") match {
      case "parquet" => createPartitionsParquet(blocks, store)
      case _ => createPartitions(blocks, store)
    }
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    // new HdfsPartitionReaderFactory(pushdown, options)
    new HdfsColumnarPartitionReaderFactory(pushdown, options, broadcastedHadoopConf)
  }
}

/** Creates a factory for creating HdfsPartitionReader objects
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class HdfsPartitionReaderFactory(pushdown: Pushdown,
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
}

/** PartitionReader of HdfsPartitions
 *
 * @param pushdown object handling filter, project and aggregate pushdown
 * @param options the options including "path"
 * @param partition the HdfsPartition to read from
 */
class HdfsPartitionReader(pushdown: Pushdown,
                          options: util.Map[String, String],
                          partition: HdfsPartition,
                          sparkSession: SparkSession,
 sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration])
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)
  val tc = TaskContext.get()
  logger.info(s"Task id: ${tc.taskAttemptId()} Stage id: ${tc.stageId} " +
              s"Partition: ${partition.index}:${partition.name}")
  /* We setup a rowIterator and then read/parse
   * each row as it is asked for.
   */
  private var store: HdfsStore = HdfsStoreFactory.getStore(pushdown, options,
                                                           sparkSession, sharedConf)
  private var rowIterator: Iterator[InternalRow] = {
    if (options.get("format") == "parquet") {
      store.getRowIterParquet(partition)
      // store.getParquetIterator(partition)
    } else {
      store.getRowIter(partition)
    }
  }

  var index = 0
  def next: Boolean = {
    rowIterator.hasNext
  }
  def get: InternalRow = {
    val row = rowIterator.next
    if (((index % 500000) == 0) ||
        (!next)) {
      logger.info(s"get: partition: ${partition.index} ${partition.offset}" +
                  s" ${partition.length} ${partition.name} index: ${index}")
    }
    index = index + 1
    row
  }

  def close(): Unit = Unit
}
