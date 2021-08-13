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
import java.io.EOFException
import java.io.InputStream
import java.nio.ByteBuffer
import java.util

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String

/** Type of object encoded in binary.
 *  This follows the encoding values used by the NDP server.
 */
object NdpDataType extends Enumeration {
  type NdpDataType = Value
  val LongType = Value(2)
  val DoubleType = Value(5)
  val StringType = Value(6)
}

/** Represents a ColumnVector which can understand
 *  the NDP columnar binary format.
 *  @param batchSize the number of items in each row of a batch.
 *  @param dataType the Int representing the NdpDataType.
 *  @param schema the schema returned from the server.
 *  @return
 */
class NdpColumnVector(batchSize: Integer, dataType: Int, schema: StructType)
    extends ColumnVector(schema: StructType) {
  private val logger = LoggerFactory.getLogger(getClass)

  val (byteBuffer: ByteBuffer,
       stringIndex: Array[Int],
       stringLen: ByteBuffer,
       bufferLength: Integer) = {
    var stringIndex = Array[Int](0)
    var stringLen = ByteBuffer.allocate(0)
    val bytes: Int = NdpDataType(dataType) match {
      case NdpDataType.LongType => 8
      case NdpDataType.DoubleType => 8
      case NdpDataType.StringType =>
      // Assumes single byte length field.
      stringIndex = new Array[Int](batchSize)
      stringLen = ByteBuffer.allocate(batchSize)
      128
      case _ => 0
    }
    (ByteBuffer.allocate(batchSize * bytes),
     stringIndex, stringLen,
     (batchSize * bytes).asInstanceOf[Integer])
  }
  def close(): Unit = {}
  def getArray(row: Int): org.apache.spark.sql.vectorized.ColumnarArray = { null }
  def getBinary(row: Int): Array[Byte] = { null }
  def getBoolean(row: Int): Boolean = { false }
  def getByte(row: Int): Byte = { byteBuffer.get(row) }
  def getChild(row: Int): org.apache.spark.sql.vectorized.ColumnVector = { null }
  def getDecimal(row: Int, r: Int, p: Int): org.apache.spark.sql.types.Decimal = { Decimal(0) }
  def getDouble(row: Int): Double = { byteBuffer.getDouble(row * 8) }
  def getFloat(row: Int): Float = { byteBuffer.getFloat(row * 8) }
  def getInt(row: Int): Int = { byteBuffer.getInt(row * 4) }
  def getLong(row: Int): Long = { byteBuffer.getLong(row * 8) }
  def getMap(row: Int): org.apache.spark.sql.vectorized.ColumnarMap = { null }
  def getShort(row: Int): Short = { byteBuffer.getShort(row * 2) }
  def getUTF8String(row: Int): org.apache.spark.unsafe.types.UTF8String = {
    val offset = stringIndex(row)
    val length = stringLen.get(row)
    UTF8String.fromBytes(byteBuffer.array(), offset, length)
  }
  def hasNull(): Boolean = { false }
  def isNullAt(row: Int): Boolean = { false }
  def numNulls(): Int = { 0 }

  /** fetches the data for a columnar batch and
   *  returns the number of rows read.
   *  The data is read into the already pre-allocated
   *  arrays for the data.  Note that if the already allocated
   *  buffers are not big enough for the data, we will throw an Exception.
   *
   *  @param stream the stream of data with NDP binary columnar format.
   *  @return Int the number of rows returned.
   */
  def readColumn(stream: DataInputStream): Int = {
    var rows: Int = 0
    try {
      var bytesRead = 0
      val numBytes = stream.readLong()
      rows = numBytes.toInt / 8
      NdpDataType(dataType) match {
        case NdpDataType.LongType =>
          if (rows > batchSize) {
            logger.warn(s"rows ${rows} > batchSize ${batchSize}")
          }
          stream.readFully(byteBuffer.array(), 0, rows * 8)
        case NdpDataType.DoubleType =>
          if (rows > batchSize) {
            logger.warn(s"rows ${rows} > batchSize ${batchSize}")
          }
          stream.readFully(byteBuffer.array(), 0, rows * 8)
        case NdpDataType.StringType =>
          rows = numBytes.toInt
          if (rows > batchSize) {
            logger.warn(s"rows ${rows} > batchSize ${batchSize}")
          }
          stream.readFully(stringLen.array(), 0, rows)
          var idx = 0
          for (i <- 0 until rows) {
            stringIndex(i) = idx
            idx += stringLen.get(i) & 0xFF
          }
          val textBytes = stream.readLong()
          if (textBytes > bufferLength) {
            logger.warn(s"textBytes ${textBytes} > bufferLength ${bufferLength}")
          }
          stream.readFully(byteBuffer.array(), 0, textBytes.toInt)
      }
    } catch {
      case ex: EOFException =>
        // logger.warn(ex.toString)
      case ex: Exception =>
        logger.warn(ex.toString)
        throw ex
    }
    rows
  }
}

object NdpColumnVector {

  /** Returns an array of NdpColumnVectors.
   *  Use of an NdpColumnVector is always in sets to represent
   *  batches of data.  Thus they are only useful in sets.
   *  This provides the api to return a relevant set of
   *  NdpColumnVectors representing the appropriate types.
   *
   *  @param batchSize the number of rows in a batch
   *  @param dataTypes the NdpDataType to use for each vector.
   *  @param schema the relevant schema for the vector.
   */
  def apply(batchSize: Integer,
            dataTypes: Array[Int],
            schema: StructType): Array[NdpColumnVector] = {
    var vectors = new Array[NdpColumnVector](dataTypes.length)
    for (i <- 0 until dataTypes.length) {
      vectors(i) = new NdpColumnVector(batchSize, dataTypes(i), schema);
    }
    vectors
  }
}
