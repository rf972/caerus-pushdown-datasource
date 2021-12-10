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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.json.{JSONException, JSONObject}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/** A scan object that works on HDFS files.
 *
 * @param options the options including "path"
 */
case class HdfsOpScan(schema: StructType,
                      options: util.Map[String, String])
      extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  private def init(): Unit = {
    if (options.get("format") == "parquet") {
      // The default for parquet is to use column names and no casts since
      // parquet knows what the data types are.
      options.put("useColumnNames", "")
      options.put("DisableCasts", "")
    }
  }
  init()
  override def toBatch: Batch = this
  private def getJsonSchema(params: String) : StructType = {
    var newSchema: StructType = new StructType()
    try {
      val jsonObject = new JSONObject(params)
      logger.info("Processor found: " + jsonObject.getString("processor"))
      val schemaJson = jsonObject.getJSONArray("schema")
      for (i <- 0 until schemaJson.length()) {
        val field = schemaJson.getJSONObject(i)
        val dataType = field.getString("type") match {
          case "StringType" => StringType
          case "IntegerType" => IntegerType
          case "DoubleType" => DoubleType
          case "LongType" => LongType
        }
        newSchema = newSchema.add(field.getString("name"), dataType, true)
      }
    } catch {
      case err: JSONException =>
        logger.error("Error " + err.toString())
    }
    newSchema
  }
  override def readSchema(): StructType = schema

  private val maxPartSize: Long = (1024 * 1024 * 128)
  private var partitions: Array[InputPartition] = Array[InputPartition]()

  private def createPartitionsParquet(blockMap: Map[String, Array[BlockLocation]],
                                      store: HdfsStore): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    val conf = new Configuration()
    val readOptions = HadoopReadOptions.builder(conf)
                                       .build()
    logger.trace("blockMap: {}", blockMap.mkString(", "))
    if ((options.containsKey("partitions") &&
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
                               rows = parquetBlock.getRowCount,
                               store.getModifiedTime(fName))
      }
    } else {
      // Generate one partition per file, per hdfs block
      for ((fName, blockList) <- blockMap) {
        // val mTime = store.getModifiedTime(fName)

        val partitions =
          if (options.containsKey("processorid")) {
            HdfsStore.getFilePartitions(fName, options.get("processorid"))
          } else {
            // If the processid is not set, we might be initializing a relation.
            // Without the intention to actually do a query.
            // If so, we will simply return 0, to avoid initting any partitions.
            0
          }
        // Generate one partition per row Group.
        for (i <- 0 to partitions - 1) {
          a += new HdfsPartition(index = i, offset = 0,
                                 length = 1,
                                 name = fName,
                                 rows = 1,
                                 0)
        }
      }
    }
    // logger.info("Partitions: " + a.mkString(", "))
    a.toArray
  }
  private val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()
  private val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(
            sparkSession.sessionState.newHadoopConf()))
  private val sqlConf = sparkSession.sessionState.conf
  /** Returns an Array of S3Partitions for a given input file.
   *  the file is selected by options("path").
   *  If there is one file, then we will generate multiple partitions
   *  on that file if large enough.
   *  Otherwise we generate one partition per file based partition.
   *
   * @return array of S3Partitions
   */
  private def getPartitions(): Array[InputPartition] = {
    var store: HdfsStore = HdfsStoreFactory.getStore(options,
                                                     sparkSession, new Configuration())
    val fileName = store.filePath
    val blocks : Map[String, Array[BlockLocation]] = store.getBlockList(fileName)
    options.get("format") match {
      case "parquet" =>
        val p = createPartitionsParquet(blocks, store)
        p
    }
  }
  override def planInputPartitions(): Array[InputPartition] = {
    if (partitions.length == 0) {
      partitions = getPartitions()
    }
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    logger.info("Create Reader Factory " + {if (options.containsKey("JsonParams")) { "Json" }
    else { "original" } })

    new HdfsBinColPartitionReaderFactory(schema, options,
                                         broadcastedHadoopConf, sqlConf)
  }
}

