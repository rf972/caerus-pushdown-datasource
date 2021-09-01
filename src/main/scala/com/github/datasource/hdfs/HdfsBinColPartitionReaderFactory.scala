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
import java.util

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/** Creates a factory for creating HdfsPartitionReader objects,
 *  for parquet files to be read using ColumnarBatches,
 *  with NDP returning its binary format.
 *
 * @param pushdown has all pushdown options.
 * @param options the options including "path"
 */
class HdfsBinColPartitionReaderFactory(schema: StructType,
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
    var store: HdfsStore = HdfsStoreFactory.getStore(options,
                                                     sparkSession, sharedConf.value.value)
    val reader =
      if (options.containsKey("ndpcompression") &&
          (options.get("ndpcompression") == "ZSTD")) {
        new HdfsCompressedColVectReader(schema, 8 * 1024,
                                        part,
                                        store.getOpStream(part).asInstanceOf[DataInputStream])
      } else {
        new HdfsBinColVectReader(schema, 4 * 1024,
                                 part,
                                 store.getOpStream(part).asInstanceOf[DataInputStream])
      }
    logger.info("HdfsBinColVectReader created row group " + part.index)
    new HdfsBinColPartitionReader(reader, batchSize)
      // This alternate factory below is identical to the above, but
      // provides more verbose progress tracking.
      // new HdfsBinColColumnarPartitionReaderProgress(vectorizedReader, batchSize, part)
  }
}

/** PartitionReader which returns a ColumnarBatch, and
 *  is relying on the HdfsBinColVectReader to
 *  fetch the batches.
 *
 * @param vectorizedReader - Already initialized HdfsBinColVectReader
 *                           which provides the data for the PartitionReader.
 */
class HdfsBinColPartitionReader(vectorizedReader: HdfsColVectReader,
                                batchSize: Integer)
  extends PartitionReader[ColumnarBatch] {
  private val logger = LoggerFactory.getLogger(getClass)
  override def next(): Boolean = vectorizedReader.next()
  override def get(): ColumnarBatch = {
    val batch = vectorizedReader.get.asInstanceOf[ColumnarBatch]
    batch
  }
  override def close(): Unit = vectorizedReader.close()
}
