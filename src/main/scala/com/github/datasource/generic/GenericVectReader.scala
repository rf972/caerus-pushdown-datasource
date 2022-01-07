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

import java.io.DataInputStream

import com.github.datasource.hdfs.{HdfsColVectReader, HdfsPartition}
import org.slf4j.LoggerFactory

import org.apache.spark.api.python.generic.ReaderIteratorExBase
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/** Allows for reading batches of columns from NDP
 *  using the NDP binary columnar format.
 *
 *  @param schema
 *  @param batchSize the number of rows in a batch
 *  @param part the current hdfs partition
 *  @param reader object used to read the data.
 */
class GenericVectReader(schema: StructType,
                        batchSize: Integer,
                        part: HdfsPartition,
                        reader: ReaderIteratorExBase[Array[Byte]]) extends HdfsColVectReader {
  private val logger = LoggerFactory.getLogger(getClass)
  def next(): Boolean = {
    nextBatch()
  }
  def get(): ColumnarBatch = {
    columnarBatch
  }
  def close(): Unit = {
  }
  private def stream: DataInputStream = {
    reader.getStream
  }
  private var rowsReturned: Long = 0
  private var batchesAvailable: Boolean = true
  private var streamCloseNeeded: Boolean = false
  private var currentBatchSize: Int = 0
  private var batchIdx: Long = 0
  // private val (numCols: Int, dataTypes: Array[DataType]) = (schema.names.length,
  //  schema.fields.map(f => f.dataType))

  private val (numCols: Integer, dataTypes: Array[Int]) = {
    /* The NDP server encodes the number of columns followed by
     * the the type of each column.  All values are doubles.
     */
    try {
      // logger.info(s"Data Read Starting ${part.name} row group ${part.index}")
      // logger.info("reading cols from stream")
      // First read the indicator of type.
      // positive values are data length, which we expect.
      val len = stream.readInt()
      len match {
        case length if length > 0 =>
        case readType => reader.handleRead(readType)
      }
      val nColsLong = stream.readLong()
      val nCols: Integer = nColsLong.toInt
      logger.info("nCols : " + String.valueOf(nCols))
      val dataTypes = new Array[Int](nCols)
      for (i <- 0 until nCols) {
        dataTypes(i) = (stream.readLong()).toInt
        logger.info(s" datatype[$i] ${dataTypes(i)}")
      }
      /* for (i <- 0 until 32) {
        val nextInt = (stream.readInt().toInt)
        logger.info(s"[$i] $nextInt")
      } */
      (nCols, dataTypes)
    } catch {
        case ex: Exception =>
        /* We do not expect to hit end of file, but if we do, it might mean that
         * the NDP query had nothing to return.
         */
          throw new Exception("Init Exception: " + ex)
          (0, new Array[Int](0))
        case ex: Throwable =>
          throw new Exception("Init Throwable: " + ex)
          (0, new Array[Int](0))
    }
  }
  private val ndpColVectors = GenericColumnVector(batchSize, dataTypes, schema)
  private val columnarBatch = new ColumnarBatch(ndpColVectors.asInstanceOf[Array[ColumnVector]])
  /** Fetches the next set of columns from the stream, returning the
   *  number of rows that were returned.
   *  We expect all columns to return the same number of rows.
   *
   *  @return Integer, the number of rows returned for the batch.
   */
  private def readNextBatch(): Integer = {
    var rows: Integer = 0
    for (i <- 0 until numCols) {
      val currentRows = ndpColVectors(i).readColumn(reader)
      if (rows == 0) {
        rows = currentRows
      } else if (rows != 0 && currentRows != rows) {
        // We expect all rows in the batch to be the same size.
        throw new Exception(s"mismatch in rows ${currentRows} != ${rows}")
      }
    }
    rows
  }
  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   * @return Boolean, true if more rows, false if none.
   */
  private def nextBatch(): Boolean = {
    if (batchesAvailable == false) {
      logger.info(s"nextBatch batchesAvailable $batchesAvailable")
      false
    }
    else if (streamCloseNeeded) {
      // For now we only allow one batch
      // On the next read we will close out the stream.
      logger.info(s"nextBatch streamCloseNeeded $streamCloseNeeded")
      val len = stream.readInt()
      len match {
        case length if length > 0 =>
        case readType => reader.handleRead(readType)
      }
      logger.info(s"nextBatch streamCloseNeeded $streamCloseNeeded Done.")
      streamCloseNeeded = false
      batchesAvailable = false
      false
    } else {
      streamCloseNeeded = true
      columnarBatch.setNumRows(0)
      val rows = readNextBatch()
      // if (rows == 0) {
      logger.info(s"nextBatch readNextBatch rows: ${rows} total: ${rowsReturned}")
      // }
      rowsReturned += rows
      columnarBatch.setNumRows(rows.toInt)
      currentBatchSize = rows
      batchIdx = 0
      if (rows > 0) {
        true
      } else {
        false
      }
    }
  }
}
