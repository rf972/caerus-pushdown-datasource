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
package com.github.datasource.generic

import java.util

import scala.collection.mutable.{ArrayBuffer}

import com.github.datasource.hdfs.{HdfsBinColPartitionReaderFactory,
  HdfsPartition}
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.{InputPartition}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/** A scan object that works on HDFS files.
 *
 * @param options the options including "path"
 */
case class GenericPushdownScan(schema: StructType,
                               options: util.Map[String, String])
  extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def toBatch: Batch = this
  override def readSchema(): StructType = schema

  private val maxPartSize: Long = (1024 * 1024 * 128)
  private var partitions: Array[InputPartition] = Array[InputPartition]()

  private def createPartitionsParquet(): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    val path = options.get("path")
    val fileList = NdpApi.getFileList(NdpApi.extractFilename(path), NdpApi.extractServer(path))
    // Generate one partition per file, per hdfs block
    for (fName <- fileList) {
      val fullPath = s"${path}/${fName}"
      val jsonStatus = NdpApi.getStatus(NdpApi.extractFilename(fullPath),
                                        NdpApi.extractServer(fullPath))
      logger.info(s"json status ${jsonStatus.toString}")
      val partitions = jsonStatus.getInt("num_row_groups")
      // Generate one partition per row Group.
      for (i <- 0 until partitions) {
        a += new HdfsPartition(index = i, offset = 0,
          length = 1,
          name = fullPath,
          rows = 1,
          0)
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
  /** Returns an Array of Partitions for a given input file.
   *  the file is selected by options("path").
   *  If there is one file, then we will generate multiple partitions
   *  on that file if large enough.
   *  Otherwise we generate one partition per file based partition.
   *
   * @return array of Partitions
   */
  private def getPartitions(): Array[InputPartition] = {
    options.get("format") match {
      case "parquet" => createPartitionsParquet()
    }
  }
  override def planInputPartitions(): Array[InputPartition] = {
    if (partitions.length == 0) {
      partitions = getPartitions()
    }
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    new HdfsBinColPartitionReaderFactory(schema, options,
      broadcastedHadoopConf, sqlConf)
  }
}
