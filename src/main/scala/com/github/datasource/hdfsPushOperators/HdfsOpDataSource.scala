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

import com.github.datasource.common.Pushdown
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
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
case class HdfsOpScan(schema: StructType,
                      inputReadSchema: StructType,
                      options: util.Map[String, String],
                      val needsRule: Boolean = true)
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
  private val readSchemaVal: StructType = {
    if (options.containsKey("JsonParams")) {
      getJsonSchema(options.get("JsonParams"))
    } else {
      inputReadSchema
    }
  }
  override def readSchema(): StructType = readSchemaVal

  protected val pushdown = new Pushdown(schema, readSchemaVal, Seq[Filter](),
                                        None, options)
  private val maxPartSize: Long = (1024 * 1024 * 128)
  private var partitions: Array[InputPartition] = getPartitions()

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
        val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(fName),
                                                                     conf), readOptions)
        val parquetBlocks = reader.getFooter.getBlocks

        // Generate one partition per row Group.
        for (i <- 0 to parquetBlocks.size - 1) {
          val parquetBlock = parquetBlocks.get(i)
          // logger.info(s"Create partition: ${fName} receivedPushdown: ${receivedPushdown}")
          a += new HdfsPartition(index = i, offset = parquetBlock.getStartingPos,
                                 length = parquetBlock.getCompressedSize,
                                 name = fName,
                                 rows = parquetBlock.getRowCount,
                                 store.getModifiedTime(fName))
        }
      }
    }
    logger.info("Partitions: " + a.mkString(", "))
    a.toArray
  }
  private val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()
  private val broadcastedHadoopConf = HdfsColumnarReaderFactory.getHadoopConf(sparkSession,
                                                                              pushdown.readSchema)
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
    var store: HdfsStore = HdfsStoreFactory.getStore(pushdown, options,
                                                     sparkSession, new Configuration())
    val fileName = store.filePath
    val blocks : Map[String, Array[BlockLocation]] = store.getBlockList(fileName)
    options.get("format") match {
      case "parquet" => createPartitionsParquet(blocks, store)
    }
  }
  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    logger.info("Create Reader Factory " + {if (options.containsKey("JsonParams")) { "Json" }
    else { "original" } })

    /* if (pushdown.schema.length == pushdown.prunedSchema.length) {
      new HdfsColumnarPartitionReaderFactory(pushdown, options,
                                             broadcastedHadoopConf, sqlConf)
    } else { */
      new HdfsBinColPartitionReaderFactory(pushdown, options,
                                           broadcastedHadoopConf, sqlConf, true)
    // }
  }
}

