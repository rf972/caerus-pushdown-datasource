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
import java.io.EOFException
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util

import scala.collection.JavaConverters._

// ZSTD support
import com.github.luben.zstd.Zstd
import org.slf4j.LoggerFactory

import org.apache.spark.api.python.generic.ReaderIteratorExBase
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String



/** Type of object encoded in binary.
 *  This follows the encoding values used by the NDP server.
 */
object NdpCompressedDataType extends Enumeration {
  type NdpCompressedDataType = Value
  val LongType = Value(2)
  val DoubleType = Value(5)
  val ByteArrayType = Value(6)
  val FixedLenByteArrayType = Value(7)
}
/** This encodes the offset of
 */
object NdpCompHeaderOffset {
  val DataType = (0 * 4)
  val TypeSize = (1 * 4)
  val DataLen = (2 * 4)
  val CompressedLen = (3 * 4)
}
/** Represents a ColumnVector which can understand
 *  the NDP columnar binary format.
 *  @param batchSize the number of items in each row of a batch.
 *  @param dataType the Int representing the NdpDataType.
 *  @param schema the schema returned from the server.
 *  @return
 */
class GenericColumnVector(batchSize: Integer, dataType: Int, schema: StructType)
    extends ColumnVector(schema: StructType) {
  private val logger = LoggerFactory.getLogger(getClass)
  // private val factory = LZ4Factory.fastestInstance()
  // private val decompressor = factory.fastDecompressor()
  // val decompressor = factory.safeDecompressor()
  var (byteBuffer: ByteBuffer,
       bufferLength: Int) = (ByteBuffer.allocate(0), 0)
  var (stringIndex: Array[Int],
       stringLen: ByteBuffer) = (Array.empty[Int], ByteBuffer.allocate(0))
  private val header = ByteBuffer.allocate(4 * 4)
  private var fixedTextLen: Int = 0
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
  def getLen1(row: Int): Int = {
    var baseIndex = fixedTextLen * row
    var l = fixedTextLen
    for (i <- 0 until fixedTextLen) {
      if (byteBuffer.get(baseIndex + i) == 0) {
        l = i
      }
    }
    l
  }
  def getFixedStringLen(row: Int): Int = {
    def findLen(i: Int, max: Int): Int = {
      if (i == max) i
      else if (byteBuffer.get(i) == 0) i
      else findLen(i + 1, max)
    }

    val i = findLen(row * fixedTextLen, ((row + 1) * fixedTextLen)) - (row * fixedTextLen)
    i
  }
  def getUTF8String(row: Int): org.apache.spark.unsafe.types.UTF8String = {
    NdpCompressedDataType(dataType) match {
       case NdpCompressedDataType.LongType => UTF8String.fromString(
                                          byteBuffer.getLong(row * 8).toString)
      case NdpCompressedDataType.DoubleType => UTF8String.fromString(
                                          byteBuffer.getDouble(row * 8).toString)
      case NdpCompressedDataType.ByteArrayType =>
        if (fixedTextLen > 0) {
          val length = getFixedStringLen(row)
          UTF8String.fromBytes(byteBuffer.array(), fixedTextLen * row, length)
        } else {
          val offset = stringIndex(row)
          val length = stringLen.get(row)
          UTF8String.fromBytes(byteBuffer.array(), offset, length)
        }
    }
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
   *  @param reader iterator used to read the data from the spark worker.py
   *  @return Int the number of rows returned.
   */
  def readColumn(reader: ReaderIteratorExBase[Array[Byte]]): Int = {
    var rows: Int = 0
    try {
      var bytesRead = 0
      rows = readColumnData(reader.getStream)
    } catch {
      case ex: EOFException =>
        // logger.warn(ex.toString)
      case ex: Exception =>
        logger.warn(ex.toString)
        throw ex
    }
    rows
  }
  /** fetches the data for a columnar batch and
   *  returns the number of rows read.
   *  The data is read into the already pre-allocated
   *  arrays for the data.  Note that if the already allocated
   *  buffers are not big enough for the data, we will throw an Exception.
   *
   *  @param stream the stream of data with NDP binary columnar format.
   *  @return Int the number of rows returned.
   */
  def readColumnData(stream: DataInputStream): Int = {
    var rows: Int = 0
    try {
      var bytesRead = 0
      stream.readFully(header.array(), 0, header.capacity())
      val numBytes = header.getInt(NdpCompHeaderOffset.CompressedLen)
      var compressed = (numBytes > 0)
      val headerDataType = header.getInt(NdpCompHeaderOffset.DataType)
      val dataBytes = header.getInt(NdpCompHeaderOffset.DataLen)
      if (dataBytes == 0) {
        rows
      } else {
        NdpCompressedDataType(headerDataType) match {
          case NdpCompressedDataType.LongType =>
            // logger.info(s"${tId}:${id}) read ${numBytes.toInt} bytes (Long)")
            byteBuffer = ByteBuffer.allocate(dataBytes)
            stream.readFully(byteBuffer.array(), 0, dataBytes)
            /* for (i <- 0 until 4) {
            logger.info(s"[$i] ${byteBuffer.getLong(i * 8)}")
          }
          for (i <- (dataBytes - 32) / 8 until dataBytes / 8) {
            logger.info(s"[$i] ${byteBuffer.getLong(i * 8)}")
          }
          for (i <- 0 until 4) {
            val nextInt = (stream.readInt().toInt)
            logger.info(s"[$i] $nextInt")
          }
          for (i <- 0 until 4) {
            logger.info(s"[$i] ${byteBuffer.getDouble(i * 8)}")
          } */
            rows = dataBytes.toInt / 8
            fixedTextLen = 0
            // logger.info(s"${tId}:${tId}:${id}) decompressed ${numBytes.toInt} bytes " +
            //             s"-> ${dataBytes} (Long)")
          case NdpCompressedDataType.DoubleType =>
            // logger.info(s"${tId}:${id}) read ${numBytes.toInt} bytes (Double)")
            byteBuffer = ByteBuffer.allocate(dataBytes)
            stream.readFully(byteBuffer.array(), 0, dataBytes)
            rows = dataBytes.toInt / 8
            fixedTextLen = 0
            // logger.info(s"${tId}:${id}) decompressed ${numBytes.toInt} " +
            //             s"-> bytes ${dataBytes} (Double)")
          case NdpCompressedDataType.FixedLenByteArrayType =>
            fixedTextLen = header.getInt(NdpCompHeaderOffset.TypeSize)
            byteBuffer = ByteBuffer.allocate(dataBytes)
            stream.readFully(byteBuffer.array(), 0, dataBytes.toInt)
            rows = dataBytes.toInt / fixedTextLen
          case NdpCompressedDataType.ByteArrayType =>
            val indexBytes = header.getInt(NdpCompHeaderOffset.DataLen)
            stringLen = ByteBuffer.allocate(indexBytes)
            stringIndex = new Array[Int](indexBytes)
            stream.readFully(stringLen.array(), 0, indexBytes.toInt)
            rows = indexBytes
            fixedTextLen = 0
            // logger.info(s"${tId}:${id}) decompressed ${numBytes.toInt} -> " +
            //             s"bytes ${indexBytes} (String Index)")
            var idx = 0
            for (i <- 0 until rows) {
              stringIndex(i) = idx
              idx += stringLen.get(i) & 0xFF
            }
            stream.readFully(header.array(), 0, header.capacity())
            val dataBytes = header.getInt(NdpCompHeaderOffset.DataLen)
            byteBuffer = ByteBuffer.allocate(dataBytes)
            stream.readFully(byteBuffer.array(), 0, dataBytes.toInt)
            // logger.info(s"${tId}:${id}) decompressed ${dataBytes.toInt} bytes " +
            //             s"-> ${dataBytes} (String)")
            for (i <- 0 until rows) {
              logger.info(s"${i}) ${getUTF8String(i)}")
            }
        }
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

object GenericColumnVector {

  /** Returns an array of GenericColumnVectors.
   *  Use of an GenericColumnVector is always in sets to represent
   *  batches of data.  Thus they are only useful in sets.
   *  This provides the api to return a relevant set of
   *  GenericColumnVectors representing the appropriate types.
   *
   *  @param batchSize the number of rows in a batch
   *  @param dataTypes the NdpDataType to use for each vector.
   *  @param schema the relevant schema for the vector.
   */
  def apply(batchSize: Integer,
            dataTypes: Array[Int],
            schema: StructType): Array[GenericColumnVector] = {
    var vectors = new Array[GenericColumnVector](dataTypes.length)
    for (i <- 0 until dataTypes.length) {
      vectors(i) = new GenericColumnVector(batchSize, dataTypes(i), schema);
    }
    vectors
  }
}
