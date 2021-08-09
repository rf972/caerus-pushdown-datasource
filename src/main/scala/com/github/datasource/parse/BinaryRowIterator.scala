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

package com.github.datasource.parse

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.DataInputStream
import java.io.EOFException
import java.io.File
import java.io.FileWriter
import java.io.InputStream
import java.io.IOException
import java.util
import java.util.Locale

import scala.collection.JavaConverters._

import com.github.datasource.common.TypeCast
import com.univocity.parsers.csv._
import org.slf4j.LoggerFactory

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Type of object encoded in binary.
 *  This follows the encoding values used by the NDP server.
 */
object BinDataType extends Enumeration {
  type BinDataType = Value
  val IntBinType = Value(1)
  val DoubleBinType = Value(2)
  val StrBinType = Value(3)
}
/** Iterator object that allows for parsing
 *  binary rows from the NDP server into InternalRow structures.
 *
 * @param inputStream the inputStream connected to the NDP server
 */
class BinaryRowIterator(inputStream: InputStream)
  extends Iterator[InternalRow] {
  private val logger = LoggerFactory.getLogger(getClass)
  private var EOFReached = false
  private val dataInputStream = inputStream.asInstanceOf[DataInputStream]
  private val (numCols: Integer, dataTypes: Array[Integer]) = {
    /* The NDP server encodes the number of columns followed by
     * the the type of each column.  All values are doubles.
     */
    try {
      val nColsLong = dataInputStream.readLong()
      val nCols: Integer = nColsLong.toInt
      // logger.info("nCols : " + String.valueOf(nCols))
      val dataTypes = new Array[Integer](nCols)
      for (i <- 0 to nCols - 1) {
        dataTypes(i) = (dataInputStream.readLong()).toInt
        // logger.info(String.valueOf(i) + " : " + String.valueOf(dataTypes(i)))
      }
      // scalastyle:on println
      (nCols, dataTypes)
    } catch {
        case ex: Exception =>
        /* We do not expect to hit end of row, but if we do, it might mean that
         * the NDP query had nothing to return.
         */
          logger.info("BinaryRowIterator Init Exception: " + ex)
          (0, new Array[Integer](0))
        case ex: Throwable =>
          logger.info("BinaryRowIterator Init Throwable: " + ex)
          (0, new Array[Integer](0))
    }
  }
  /** Returns an InternalRow parsed from binary of InputStream.
   *
   * @return the InternalRow of this line.
   */
  private def parseRow(): InternalRow = {
    var index = 0
    var row = new Array[Any](numCols)
    try {
      for (i <- 0 to numCols - 1) {
        BinDataType(dataTypes(i)) match {
          case BinDataType.IntBinType =>
            val intData = dataInputStream.readLong()
            row(i) = intData
          case BinDataType.DoubleBinType =>
            val doubleData = dataInputStream.readDouble()
            row(i) = doubleData.toDouble
          case BinDataType.StrBinType =>
            val strData = dataInputStream.readUTF()
            row(i) = UTF8String.fromString(strData)
        }
        index += 1
      }
    } catch {
      case ex: EOFException =>
        EOFReached = true
        // logger.warn(ex.toString)
      case ex: Exception =>
        logger.warn(ex.toString)
    }
    if (index >= numCols) {
      // logger.info("row: " + row.mkString(", "))
      new GenericInternalRow(row)
    } else {
      /* If empty, we will simply discard the row since
       * it is possible that we hit EOF.
       */
      InternalRow.empty
    }
  }
  /** Returns the next row or if none, InternalRow.empty.
   *
   * @return InternalRow for the next row.
   */
  private var nextRow: InternalRow = {
    val firstRow = getNextRow()
    firstRow
  }
  /** Returns row following the current one,
   *  (if availble), by parsing the next line.
   *
   * @return the next InternalRow object or InternalRow.empty if none.
   */
  private var lastRow: InternalRow = InternalRow.empty
  private def getNextRow(): InternalRow = {
    var line: String = null
    if (EOFReached) {
      // If we hit EOF, there is nothing left, so just return the empty row.
      InternalRow.empty
    } else {
      // Parse and return the next row.
      parseRow()
    }
  }
  /** Returns true if there are remaining rows.
   *
   * @return true if rows remaining, false otherwise.
   */
  override def hasNext: Boolean = {
    nextRow.numFields > 0
  }
  /** Returns the following InternalRow
   *
   * @return the next InternalRow or InternalRow.empty if none.
   */
  override def next: InternalRow = {
    val row = nextRow
    nextRow = getNextRow()
    row
  }
  def close(): Unit = {}
}

/** Contains related methods for the BinaryRowIterator class.
 */
object BinaryRowIterator {
  /** Returns the Iterator object which can process the
   *  binary information returned from the NDP server.
   *
   * @param inputStream The input stream attached to the NDP server.
   * @return a new Iterator of InternalRow constructed with above parameters.
   */
  def apply(inputStream: InputStream): Iterator[InternalRow] = {
    new BinaryRowIterator(inputStream)
  }
}
