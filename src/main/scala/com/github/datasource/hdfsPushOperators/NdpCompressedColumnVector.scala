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
import java.lang.Thread
import java.nio.ByteBuffer
import java.util

import scala.collection.JavaConverters._

// LZ4 support
import net.jpountz.lz4.LZ4Exception
import net.jpountz.lz4.LZ4Factory
import net.jpountz.lz4.LZ4FastDecompressor
import net.jpountz.lz4.LZ4SafeDecompressor
import net.jpountz.xxhash.XXHashFactory
import org.slf4j.LoggerFactory

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
 *  @param dataType the Int representing the NdpCompressedDataType.
 *  @param schema the schema returned from the server.
 *  @return
 */
class NdpCompressedColumnVector(batchSize: Integer, dataType: Int, schema: StructType, id: String)
    extends ColumnVector(schema: StructType) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val factory = LZ4Factory.fastestInstance()
  private val decompressor = factory.fastDecompressor()
  // val decompressor = factory.safeDecompressor()
  val (byteBuffer: ByteBuffer,
       bufferLength: Integer,
       compressedBuffer: ByteBuffer,
       compressedBufLen: Integer,
       stringIndex: Array[Int],
       stringLen: ByteBuffer) = {
    var stringIndex = Array[Int](0)
    var stringLen = ByteBuffer.allocate(0)
    val bytes: Int = NdpCompressedDataType(dataType) match {
      case NdpCompressedDataType.LongType => 8
      case NdpCompressedDataType.DoubleType => 8
      case NdpCompressedDataType.ByteArrayType =>
      // Assumes single byte length field.
      stringIndex = new Array[Int](batchSize)
      stringLen = ByteBuffer.allocate(batchSize)
      128
      case _ => 0
    }
    (ByteBuffer.allocate(batchSize * bytes),
     (batchSize * bytes).asInstanceOf[Integer],
     ByteBuffer.allocate(batchSize * bytes),
     (batchSize * bytes).asInstanceOf[Integer],
     stringIndex, stringLen)
  }
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
  def getUTF8String(row: Int): org.apache.spark.unsafe.types.UTF8String = {
    NdpCompressedDataType(dataType) match {
       case NdpCompressedDataType.LongType => UTF8String.fromString(
                                          byteBuffer.getLong(row * 8).toString)
      case NdpCompressedDataType.DoubleType => UTF8String.fromString(
                                          byteBuffer.getDouble(row * 8).toString)
      case NdpCompressedDataType.ByteArrayType =>
        if (fixedTextLen > 0) {
          UTF8String.fromBytes(byteBuffer.array(), fixedTextLen * row, fixedTextLen)
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
   *  @param stream the stream of data with NDP binary columnar format.
   *  @return Int the number of rows returned.
   */
  def readColumn(stream: DataInputStream): Int = {
    var rows: Int = 0
    try {
      var bytesRead = 0
      stream.readFully(header.array(), 0, header.capacity())

      val numBytes = header.getInt(NdpCompHeaderOffset.CompressedLen)
      // val tId = Thread.currentThread().getId()
      NdpCompressedDataType(header.getInt(NdpCompHeaderOffset.DataType)) match {
        case NdpCompressedDataType.LongType =>
          if (numBytes.toInt > compressedBufLen) {
            throw new Exception(s"numBytes (${numBytes.toInt}) > " +
                                s"compressedBufLen (${compressedBufLen}")
          }
          // logger.info(s"${tId}:${id}) read ${numBytes.toInt} bytes (Long)")
          stream.readFully(compressedBuffer.array(), 0, numBytes.toInt)
          // logger.info(s"${tId}:${id}) decompressing (Double)")
          val dataBytes = header.getInt(NdpCompHeaderOffset.DataLen)
          decompressor.decompress(compressedBuffer.array(), 0,
                                  byteBuffer.array(), 0, dataBytes.toInt)
          rows = dataBytes.toInt / 8
          fixedTextLen = 0
          // logger.info(s"${tId}:${tId}:${id}) decompressed ${numBytes.toInt} bytes " +
          //             s"-> ${dataBytes} (Long)")
          if (rows > batchSize) {
            throw new Exception(s"rows ${rows} > batchSize ${batchSize}")
          }
        case NdpCompressedDataType.DoubleType =>
          if (numBytes.toInt > compressedBufLen) {
            throw new Exception(s"numBytes (${numBytes.toInt}) > " +
                                s"compressedBufLen (${compressedBufLen}")
          }
          // logger.info(s"${tId}:${id}) read ${numBytes.toInt} bytes (Double)")
          stream.readFully(compressedBuffer.array(), 0, numBytes.toInt)
          // logger.info(s"${tId}:${id}) decompressing (Double)")
          val dataBytes = header.getInt(NdpCompHeaderOffset.DataLen)
          decompressor.decompress(compressedBuffer.array(), 0,
                                  byteBuffer.array(), 0, dataBytes.toInt)
          rows = dataBytes.toInt / 8
          fixedTextLen = 0
          // logger.info(s"${tId}:${id}) decompressed ${numBytes.toInt} " +
          //             s"-> bytes ${dataBytes} (Double)")
          if (rows > batchSize) {
            throw new Exception(s"rows ${rows} > batchSize ${batchSize}")
          }
        case NdpCompressedDataType.FixedLenByteArrayType =>
          fixedTextLen = header.getInt(NdpCompHeaderOffset.TypeSize)
          if (numBytes.toInt > compressedBufLen) {
            throw new Exception(s"numBytes (${numBytes.toInt}) > " +
                                s"compressedBufLen (${compressedBufLen}")
          }
          stream.readFully(compressedBuffer.array(), 0, numBytes.toInt)
          val dataBytes = header.getInt(NdpCompHeaderOffset.DataLen)
          if (dataBytes.toInt > bufferLength) {
            throw new Exception(s"dataBytes ${dataBytes.toInt} > bufferLength ${bufferLength}")
          }
          decompressor.decompress(compressedBuffer.array(), 0,
                                  byteBuffer.array(), 0, dataBytes)
          rows = dataBytes.toInt / fixedTextLen
        case NdpCompressedDataType.ByteArrayType =>
          if (numBytes.toInt > compressedBufLen) {
            throw new Exception(s"numBytes (${numBytes.toInt}) > " +
                                s"compressedBufLen (${compressedBufLen}")
          }
          // Read and decompress string index.
          stream.readFully(compressedBuffer.array(), 0, numBytes.toInt)
          // logger.info(s"${tId}:${id},${tId}) read ${numBytes.toInt} bytes (String Index)")
          // logger.info(s"${tId}:${id}) decompressing (String Index)")
          val indexBytes = header.getInt(NdpCompHeaderOffset.DataLen)
          decompressor.decompress(compressedBuffer.array(), 0,
                                  stringLen.array(), 0, indexBytes)
          rows = indexBytes
          fixedTextLen = 0
          // logger.info(s"${tId}:${id}) decompressed ${numBytes.toInt} -> " +
          //             s"bytes ${indexBytes} (String Index)")
          if (rows > batchSize) {
            throw new Exception(s"rows ${rows} > batchSize ${batchSize}")
          }
          var idx = 0
          for (i <- 0 until rows) {
            stringIndex(i) = idx
            idx += stringLen.get(i) & 0xFF
          }
          stream.readFully(header.array(), 0, header.capacity())
          val textBytes = header.getInt(NdpCompHeaderOffset.CompressedLen)
          if (textBytes.toInt > compressedBufLen) {
            throw new Exception(s"numBytes (${textBytes.toInt}) > " +
                                s"compressedBufLen (${compressedBufLen}")
          }
          stream.readFully(compressedBuffer.array(), 0, textBytes.toInt)
          // logger.info(s"${tId}:${id}) read ${textBytes.toInt} bytes (String)")
          // logger.info(s"${tId}:${id}) decompressing (String)")
          val dataBytes = header.getInt(NdpCompHeaderOffset.DataLen)
          decompressor.decompress(compressedBuffer.array(), 0,
                                  byteBuffer.array(), 0, dataBytes)
          // logger.info(s"${tId}:${id}) decompressed ${dataBytes.toInt} bytes " +
          //             s"-> ${dataBytes} (String)")
          if (dataBytes > bufferLength) {
            throw new Exception(s"textBytes ${textBytes} > bufferLength ${bufferLength}")
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

object NdpCompressedColumnVector {
  var colIndex = 0
  /** Returns an array of NdpCompressedColumnVectors.
   *  Use of an NdpCompressedColumnVector is always in sets to represent
   *  batches of data.  Thus they are only useful in sets.
   *  This provides the api to return a relevant set of
   *  NdpCompressedColumnVectors representing the appropriate types.
   *
   *  @param batchSize the number of rows in a batch
   *  @param dataTypes the NdpCompressedDataType to use for each vector.
   *  @param schema the relevant schema for the vector.
   */
  def apply(batchSize: Integer,
            dataTypes: Array[Int],
            schema: StructType): Array[NdpCompressedColumnVector] = {
    var vectors = new Array[NdpCompressedColumnVector](dataTypes.length)
    for (i <- 0 until dataTypes.length) {
      val id = f"$colIndex%d) $i%d/${dataTypes.length}%d"
      colIndex += 1
      vectors(i) = new NdpCompressedColumnVector(batchSize, dataTypes(i), schema, id);
    }
    vectors
  }
}
