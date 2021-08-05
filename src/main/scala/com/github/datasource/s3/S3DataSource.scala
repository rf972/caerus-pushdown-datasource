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
package com.github.datasource.s3

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

import com.amazonaws.services.s3.model.S3ObjectSummary
import com.github.datasource.common.Pushdown
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/** A scan object that works on S3 files.
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class S3Scan(schema: StructType,
             options: util.Map[String, String],
             filters: Array[Filter], prunedSchema: StructType,
             pushedAggregation: Option[Aggregation])
      extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)

  override def toBatch: Batch = this

  protected val pushdown = new Pushdown(schema, prunedSchema, filters,
                                      pushedAggregation, options)
  override def readSchema(): StructType = pushdown.readSchema
  private val spark = SparkSession.builder()
                                  .getOrCreate()
  private val maxPartBytes: Long = spark.sessionState.conf.filesMaxPartitionBytes
  private var partitions: Array[InputPartition] = getPartitions()

  private def generateFilePartitions(objectSummary : S3ObjectSummary): Array[InputPartition] = {
    var store: S3Store = S3StoreFactory.getS3Store(pushdown, options)
    var numPartitions: Int =
      if (options.containsKey("partitions") &&
          options.get("partitions").toInt != 0) {
        options.get("partitions").toInt
      } else {
        // Support a number of partitions when the size of the file is
        // larger than maxPartBytes.  In this case, each partition is maxPartBytes, and
        // the last partition is the remaining bytes.
        ( (objectSummary.getSize() / maxPartBytes) +
         (if ((objectSummary.getSize() % maxPartBytes) == 0) 0 else 1)).toInt
      }
    if (numPartitions == 0) {
      throw new ArithmeticException("numPartitions is 0")
    }
    var partitionArray = new ArrayBuffer[InputPartition](0)
    var offsetBytes: Long = 0
    logger.debug(s"""Num Partitions ${numPartitions}""")
    for (i <- 0 to (numPartitions - 1)) {
      val lengthBytes = {
        if ((objectSummary.getSize() - offsetBytes) > maxPartBytes) {
          maxPartBytes
        }
        else {
          objectSummary.getSize() - offsetBytes
        }
      }
      val nextPart = new S3Partition(index = i,
                                     offsetBytes = offsetBytes,
                                     lengthBytes = lengthBytes,
                                     onlyPartition = (numPartitions == 1),
                                     bucket = objectSummary.getBucketName(),
                                     key = objectSummary.getKey()).asInstanceOf[InputPartition]
      partitionArray += nextPart
      offsetBytes += lengthBytes
      logger.info(nextPart.toString)
    }
    partitionArray.toArray
  }
  private def createS3Partitions(objectSummaries : Array[S3ObjectSummary]):
    Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    // In this case we generate one partition per file.
    for (summary <- objectSummaries) {
      a += new S3Partition(index = i, bucket = summary.getBucketName(), key = summary.getKey())
      i += 1
    }
    logger.info(a.mkString(" "))
    a.toArray
  }

  /** Returns an Array of S3Partitions for a given input file.
   *  the file is selected by options("path").
   *  If there is one file, then we will generate multiple partitions
   *  on that file if large enough.
   *  Otherwise we generate one partition per file based partition.
   *
   * @return array of S3Partitions
   */
  private def getPartitions(): Array[InputPartition] = {
    var store: S3Store = S3StoreFactory.getS3Store(pushdown, options)
    val objectSummaries : Array[S3ObjectSummary] = store.getObjectSummaries()

    // If there is only one file, we will partition it as needed automatically.
    if (objectSummaries.length == 1) {
      generateFilePartitions(objectSummaries(0))
    } else {
      // If there are multiple files we treat each one as a partition.
      createS3Partitions(objectSummaries)
    }
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory =
          new S3PartitionReaderFactory(pushdown, options)
}

/** Creates a factory for creating S3PartitionReader objects
 *
 * @param pushdown object handling filter, project and aggregate pushdown
 * @param options the options including "path"
 */
class S3PartitionReaderFactory(pushdown: Pushdown,
                               options: util.Map[String, String])
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new S3PartitionReader(pushdown, options, partition.asInstanceOf[S3Partition])
  }
}

