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
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/** PartitionReader of S3Partitions
 *
 * @param pushdown object handling filter, project and aggregate pushdown
 * @param options the options including "path"
 */
class S3PartitionReader(pushdown: Pushdown,
                        options: util.Map[String, String],
                        partition: S3Partition)
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)

  /* We setup a rowIterator and then read/parse
   * each row as it is asked for.
   */
  private val store: S3Store = S3StoreFactory.getS3Store(pushdown, options)
  private var rowIterator: Iterator[InternalRow] = store.getRowIter(partition)
  private var rows: Long = if (options.containsKey("EnableProgress")) {
    store.getNumRows
  } else {
    Long.MaxValue
  }
  var index: Long = 0
  def next: Boolean = {
    rowIterator.hasNext
  }
  def get: InternalRow = {
    val row = rowIterator.next
    if (((index % 500000) == 0) ||
        (!next)) {
      val pct: String = {
        if (rows == Long.MaxValue ||
            rows == 0) {
          ""
        } else {
          val percentage = ((index * 100.toDouble) / rows.toDouble)
          s"/${rows} " + "%.2f".format(percentage).toDouble.toString + " %"
        }
      }
      logger.info(s"get: partition: ${partition.index} ${partition.bucket} " +
                  s"${partition.key} index: ${index}${pct}")
    }
    index = index + 1
    row
  }

  def close(): Unit = Unit
}
