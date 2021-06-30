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

import java.io.ByteArrayOutputStream
import java.net.URI

import scala.collection.mutable.ArrayBuffer

import com.github.datasource.common.Pushdown
import com.github.datasource.common.SeekableByteArrayInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.log4j.Logger
import org.apache.parquet.hadoop.util.HadoopStreams
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.dike.hdfs.NdpHdfsFileSystem
import org.slf4j.LoggerFactory

import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils

/** An InputFile which can interface with the Ndp server.
 *
 *  @param store object for opening the stream.
 *  @param partition the partition to open.
 *
 */
class HdfsNdpInputFile(store: HdfsStore, partition: HdfsPartition) extends InputFile {
  protected val logger = LoggerFactory.getLogger(getClass)
  override def getLength(): Long = {
   partition.length
  }
  /* def newStreamDefault(): SeekableInputStream = {
    val iStream = fs.open(stat.getPath())
    val data = new Array[Byte](stat.getLen().asInstanceOf[Int])
    var bytesRead = iStream.read(data, 0, data.length)
    HadoopStreams.wrap(new FSDataInputStream(new SeekableByteArrayInputStream(data)))
  } */
  override def newStream(): SeekableInputStream = {
    val outBufStream = new ByteArrayOutputStream()
    val data = new Array[Byte](1024*1024)
    // val stream = fs.open(stat.getPath())
    val stream = store.open(partition)
    var bytesRead = stream.read(data, 0, data.length)

    while (bytesRead != -1) {
      outBufStream.write(data, 0, bytesRead)
      bytesRead = stream.read(data, 0, data.length)
    }
    HadoopStreams.wrap(new FSDataInputStream(
              new SeekableByteArrayInputStream(outBufStream.toByteArray())))
  }
  override def toString(): String = {
    partition.name.toString()
  }
}

/** Related methods.
 */
object HdfsNdpInputFile {
  protected val logger = LoggerFactory.getLogger(getClass)

  def fromPath(store: HdfsStore, partition: HdfsPartition): HdfsNdpInputFile = {
    new HdfsNdpInputFile(store, partition)
  }
}
