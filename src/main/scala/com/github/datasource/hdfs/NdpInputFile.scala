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

class NdpInputFile(fs: FileSystem, stat: FileStatus, conf: Configuration) extends InputFile {
  protected val logger = LoggerFactory.getLogger(getClass)
  override def getLength(): Long = {
    stat.getLen()
  }
  def altnewStream(): SeekableInputStream = {
    val iStream = fs.open(stat.getPath())
    val data = new Array[Byte](stat.getLen().asInstanceOf[Int])
    var bytesRead = iStream.read(data, 0, data.length)
    HadoopStreams.wrap(new FSDataInputStream(new SeekableByteArrayInputStream(data)))
  }
  override def newStream(): SeekableInputStream = {
    val outBufStream = new ByteArrayOutputStream()
    val data = new Array[Byte](1024*1024)
    val iStream = fs.open(stat.getPath())
    var bytesRead = iStream.read(data, 0, data.length)

    while (bytesRead != -1) {
      outBufStream.write(data, 0, bytesRead)
      bytesRead = iStream.read(data, 0, data.length)
    }
    HadoopStreams.wrap(new FSDataInputStream(
              new SeekableByteArrayInputStream(outBufStream.toByteArray())))
  }
  override def toString(): String = {
    stat.getPath().toString()
  }
}

object NdpInputFile {
  protected val logger = LoggerFactory.getLogger(getClass)
  private def getEndpoint(path: String): String = {
    val server = path.split("/")(2)
    if (path.contains("ndphdfs://")) {
      ("ndphdfs://" + server + {if (path.contains(":9860")) "" else ":9860"})
    } else if (path.contains("webhdfs://")) {
      ("webhdfs://" + server + {if (path.contains(":9870")) "" else ":9870"})
    } else {
      ("hdfs://" + server + {if (path.contains(":9000")) "" else ":9000"})
    }
  }
  private def getFilePath(filePath: String): String = {
    val server = filePath.split("/")(2)
    if (filePath.contains("ndphdfs://")) {
      val str = filePath.replace("ndphdfs://" + server,
                                 "ndphdfs://" + server +
                                 {if (filePath.contains(":9860")) "" else ":9860"})
      str
    } else if (filePath.contains("webhdfs")) {
      filePath.replace(server, server + {if (filePath.contains(":9870")) "" else ":9870"})
    } else {
      filePath.replace(server, server + {if (filePath.contains(":9000")) "" else ":9000"})
    }
  }
  private val configuration: Configuration = {
    val conf = new Configuration()
    conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    conf.set("dfs.client.cache.readahead", "0")
    conf.set("fs.ndphdfs.impl", classOf[org.dike.hdfs.NdpHdfsFileSystem].getName)
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    conf
  }
  private def getFileSystem(filePath: String): FileSystem = {
    val conf = configuration
    val endpoint = getEndpoint(filePath)
    if (filePath.contains("ndphdfs")) {
      val fs = FileSystem.get(URI.create(endpoint), conf)
      fs.asInstanceOf[NdpHdfsFileSystem]
    } else {
      FileSystem.get(URI.create(endpoint), conf)
    }
  }
  def fromPath(path: String) : NdpInputFile = {
    val conf = configuration
    val fs: FileSystem = getFileSystem(path)
    new NdpInputFile(fs, fs.getFileStatus(new Path(getFilePath(path))), conf)
  }
  def fromStatus(stat: FileStatus, conf: Configuration): NdpInputFile = {
    val fs: FileSystem = stat.getPath().getFileSystem(conf)
    new NdpInputFile(fs, stat, conf)
  }
}
