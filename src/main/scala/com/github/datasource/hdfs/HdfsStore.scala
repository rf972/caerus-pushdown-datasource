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

import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.DataInputStream
import java.io.FileInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util
import java.util.HashMap
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.commons.csv._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetRecordReader
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type
import org.dike.hdfs.NdpHdfsFileSystem
import org.slf4j.LoggerFactory

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, InterpretedOrdering}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types._

/** A Factory to fetch the correct type of
 *  store object.
 */
object HdfsStoreFactory {
  /** Returns the store object.
   *
   * @param options the parameters including those to construct the store
   * @return a new HdfsStore object constructed with above parameters.
   */
  def getStore(options: java.util.Map[String, String],
               sparkSession: SparkSession,
               sharedConf: Configuration):
      HdfsStore = {
    new HdfsStore(options, sparkSession, sharedConf)
  }
}
/** A hdfs store object which can connect
 *  to a file on hdfs filesystem, specified by options("path"),
 *  And which can read a partition with any of various pushdowns.
 *
 * @param options the parameters including those to construct the store
 */
class HdfsStore(options: java.util.Map[String, String],
                sparkSession: SparkSession,
                sharedConf: Configuration) {
  protected val logger = LoggerFactory.getLogger(getClass)
  override def toString() : String = "HdfsStore" + options
  protected val path = options.get("path")
  protected val origServerFull = path.split("[/:]").filter(_.nonEmpty)(1)
  protected val origServerPort = path.split("[/]").filter(_.nonEmpty)(1)
  /* HDFS_PATH is the location of the HDFS server.
   */
  protected val server = sys.env.getOrElse("HDFS_PATH", origServerFull)
  logger.info("path is " + path)
  logger.info("server is " + server)
  protected val endpoint = {
    if (path.contains("ndphdfs://")) {
      ("ndphdfs://" + server + ":9860")
    } else if (path.contains("webhdfs://")) {
      ("webhdfs://" + server + ":9870")
    } else {
      ("hdfs://" + server + ":9000")
    }
  }
  val filePath = getFilePath(path)

  def getFilePath(path: String): String = {
    if (path.contains("ndphdfs://")) {
      val serverPort = path.split("[/]").filter(_.nonEmpty)(1)
      val str = path.replace("ndphdfs://" + serverPort, "ndphdfs://" + server + ":9860")
      str
    } else if (path.contains("webhdfs")) {
      path.replace(origServerPort, server + ":9870")
    } else {
      path.replace(origServerPort, server + ":9000")
    }
  }
  protected val configure: Configuration = {
    /* val conf = new Configuration()
    conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    conf.set("dfs.client.cache.readahead", "0") */
    sharedConf.set("fs.ndphdfs.impl", classOf[org.dike.hdfs.NdpHdfsFileSystem].getName)
    sharedConf.set("dfs.client.use.datanode.hostname", "true")
    sharedConf
  }
  protected val fileSystem = {
    val conf = configure
    if (path.contains("ndphdfs")) {
      val fs = FileSystem.get(URI.create(endpoint), conf)
      fs.asInstanceOf[NdpHdfsFileSystem]
    } else {
      FileSystem.get(URI.create(endpoint), conf)
    }
  }

  /** Returns a list of BlockLocation object representing
   *  all the hdfs blocks in a file.
   *
   * @param fileName the full filename path
   * @return Map[String, BlockLocation] The Key is the filename
   *                     the value is the Array of BlockLocation
   */
  def getBlockList(fileName: String) : scala.collection.immutable.Map[String,
    Array[BlockLocation]] = {
    val fileToRead = new Path(fileName.replace(origServerPort, server))
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    val blockMap = scala.collection.mutable.Map[String, Array[BlockLocation]]()
    if (fileSystem.isFile(fileToRead)) {
      // Use MaxValue to indicate we want info on all blocks.
      blockMap(fileName) = fileSystem.getFileBlockLocations(fileToRead, 0, Long.MaxValue)
    } else {
      /* fileToRead is a directory. So get the contents of this directory.
       * For each file in the directory create a new map entry with filename as key.
       */
      val status = fileSystem.listStatus(fileToRead)
      for (item <- status) {
        if (item.isFile && (item.getPath.getName.contains(".csv") ||
                            item.getPath.getName.contains(".tbl") ||
                            item.getPath.getName.contains(".parquet"))) {
          val currentFile = item.getPath.toString
          // Use MaxValue to indicate we want info on all blocks.
          blockMap(currentFile) = fileSystem.getFileBlockLocations(item.getPath, 0, Long.MaxValue)
        }
      }
    }
    blockMap.toMap
  }

  /** Returns the length of the file in bytes.
   *
   * @param fileName the full path of the file
   * @return byte length of the file.
   */
  def getLength(fileName: String) : Long = {
    val fileToRead = new Path(fileName)
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    fileStatus.getLen
  }

  /** Returns the last modification time of the file.
   *
   * @param fileName the full path of the file
   * @return modified time of the file.
   */
  def getModifiedTime(fileName: String) : Long = {
    val fileToRead = new Path(filePath)
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    // logger.info("fileStatus {}", fileStatus.toString)
    fileStatus.getModificationTime
  }
  /** Returns an InputStream on an NDP server.
   * @param partition the current partition
   * @return InputStream for this partition, including pushdown
   */
  def getOpStream(partition: HdfsPartition): InputStream = {
    logger.info(s"stream partition: ${partition.toString}")
    val readParam = {
      options.get("format") match {
        case "parquet" =>
          val columnList = options.getOrDefault("ndpprojectcolumns", "").split(",")
          val fileName = partition.name.replace("ndphdfs://dikehdfs:9860", "")
          val compression = options.getOrDefault("ndpcompression", "None")
          val compLevel = options.getOrDefault("ndpcomplevel", "-100")
          val test = options.getOrDefault("currenttest", "Unknown Test")
          val filters = options.getOrDefault("ndpjsonfilters", "")
          val lambdaXml = new ProcessorRequestLambda(partition.modifiedTime, partition.index,
                                                     columnList, fileName,
                                                     compression, compLevel,
                                                     filters, test).toXml
          logger.info(lambdaXml.replace("\n", "").replace("  ", ""))
          lambdaXml
      }
    }
    val fs = fileSystem.asInstanceOf[NdpHdfsFileSystem]
    // logger.info("open fs rowGroup " + partition.index)
    // HdfsStore.logStart(partition.index)
    val inFile = getFilePath(partition.name)
    val inStrm = fs.open(new Path(inFile), 4096, readParam).asInstanceOf[FSDataInputStream]
    new DataInputStream(new BufferedInputStream(inStrm, 128*1024))
  }
}

/** Related routines for the HDFS connector.
 *
 */
object HdfsStore {

  protected val logger = LoggerFactory.getLogger(getClass)
  private val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()

  /** Returns true if pushdown is supported by this flavor of
   *  filesystem represented by a string of "filesystem://filename".
   *
   * @param options map containing "path".
   * @return true if pushdown supported, false otherwise.
   */
  def pushdownSupported(options: util.Map[String, String]): Boolean = {
    if (options.get("path").contains("ndphdfs://")) {
      true
    } else if (options.get("format").contains("parquet")) {
      // Regular hdfs does pushdown in the datasource.
      true
    } else {
      // Other filesystems like hdfs and webhdfs do not support pushdown.
      false
    }
  }

  /** Returns true if a filter can be fully pushed down.  This occurs when
   *  the entity we pushdown to guarantees that the filter does not need
   *  to be re-evaluated on the results returned.
   *  It is worth noting that if we cannot fully pushdown filters, then
   *  Spark will not consider pushing down aggregates.
   *
   * @param options map containing "path".
   * @return true if pushdown supported, false otherwise.
   */
  def filterPushdownFullySupported(options: util.Map[String, String]): Boolean = {
    if (options.get("path").startsWith("hdfs://") &&
        options.get("format").contains("parquet")) {
      // With parquet, the filters need to be re-evaluated.
      false
    } else {
      // everything else supports full pushdown
      true
    }
  }
  /** returns a string with the pathname of the hdfs object,
   *  including the server port.  The user might not include the port,
   *  and if it is missing, we will add it.
   *
   *  @param filePath the hdfs filename.
   * @return String the new file path in hdfs format with server port.
   */
  def getFilePath(filePath: String): String = {
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
  /** Returns a list of file names represented by an input file or directory.
   *
   * @param fileName the full filename path
   * @return Map[String, BlockLocation] The Key is the filename
   *                     the value is the Array of BlockLocation
   */
  def getFileList(fileName: String, fileSystem: FileSystem):
      Seq[String] = {
    val fileToRead = new Path(getFilePath(fileName))
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    var fileArray: Array[String] = Array[String]()
    if (fileSystem.isFile(fileToRead)) {
      // Use MaxValue to indicate we want info on all blocks.
      fileArray = fileArray ++ Array(fileName)
    } else {
      /* fileToRead is a directory. So get the contents of this directory.
       * For each file in the directory create a new map entry with filename as key.
       */
      val status = fileSystem.listStatus(fileToRead)
      for (item <- status) {
        if (item.isFile && (item.getPath.getName.contains(".csv") ||
                            item.getPath.getName.contains(".tbl") ||
                            item.getPath.getName.contains(".parquet"))) {
          val currentFile = item.getPath.toString
          fileArray = fileArray ++ Array(currentFile)
        }
      }
    }
    fileArray.toSeq
  }
  private val configuration: Configuration = {
    val conf = sparkSession.sessionState.newHadoopConf()
    // conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    // conf.set("dfs.client.cache.readahead", "0")
    conf.set("fs.ndphdfs.impl", classOf[org.dike.hdfs.NdpHdfsFileSystem].getName)
    // conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
    // conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    conf
  }
  /** Extracts the endpoint, aka server name and port from the file path.
   *
   *  @param path The pathname to extract the endpoint from.
   *  @return String the endpoint.
   */
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
  /** Fetches the FileSystem for this file string
   *  @param filePath the file pathname to get the hdfs filesystem for.
   *  @return FileSystem
   */
  def getFileSystem(filePath: String): FileSystem = {
    val conf = configuration
    getFileSystem(filePath, conf)
  }
  /** Fetches the FileSystem for this file string
   *  @param filePath the file pathname to get the hdfs filesystem for.
   *  @param conf the hdfs configuration.
   *  @return FileSystem
   */
  def getFileSystem(filePath: String, conf: Configuration): FileSystem = {
    val endpoint = getEndpoint(filePath)
    if (filePath.contains("ndphdfs")) {
      val fs = FileSystem.get(URI.create(endpoint), conf)
      fs.asInstanceOf[NdpHdfsFileSystem]
    } else {
      FileSystem.get(URI.create(endpoint), conf)
    }
  }
  /** Fetches a list of FileStatus objects for this directory, or
   *  if the filePath is a file, just a list containing the file's FileStatus.
   *  @param filePath the file or directory path.
   *  @return Seq[FileStatus]
   */
  def getFileStatusList(filePath: String): Seq[FileStatus] = {
      val conf = configuration
      val fs: FileSystem = getFileSystem(filePath, conf)
      val files = HdfsStore.getFileList(filePath, fs)
      val fileStatusArray = {
        var statusArray = Array[FileStatus]()
        for (file <- files) {
          val fileStatus = fs.getFileStatus(new Path(getFilePath(file)))
          statusArray = statusArray ++ Array(fileStatus)
        }
        statusArray.toSeq
      }
      fileStatusArray
  }
  def getFilePartitions(filePath: String): Integer = {
    val files = Map("lineitem.parquet" -> 172,
                    "part.parquet" -> 5)
    var partitions = 0
    for ((f, v) <- files) {
      if (filePath.contains(f)) {
        partitions = files(f)
        logger.info(s"found {} partitions for file {}", partitions, filePath)
      }
    }
    if (partitions == 0) {
      val conf = new Configuration()
      val readOptions = HadoopReadOptions.builder(conf)
                                         .build()
      val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath),
                                                                   conf), readOptions)
      val parquetBlocks = reader.getFooter.getBlocks
      partitions = parquetBlocks.size
      logger.info(s"parquet MR {} partitions for file {}", partitions, filePath)
    }
    partitions
  }
  /** Sends a read ahead operation to NDP Server
   * @param options all parameters for the request, including
   *                path of the file, json filters, and json aggregate
   */
  def sendReadAhead(options: HashMap[String, String]): Unit = {
    val filePath = options.get("path")
    val readParam = {
      options.get("format") match {
        case "parquet" =>
          val columnList = options.getOrDefault("ndpprojectcolumns", "").split(",")
          val fName = filePath.replace("ndphdfs://dikehdfs:9860", "")
          val compression = options.getOrDefault("ndpcompression", "None")
          val compLevel = options.getOrDefault("ndpcomplevel", "-100")
          val test = options.getOrDefault("currenttest", "Unknown Test")
          val filters = options.getOrDefault("ndpjsonfilters", "")
          val lambdaXml = new ProcessorRequestLambda(0, 0,
                                                     columnList, fName,
                                                     compression, compLevel,
                                                     filters, test).toXml
          logger.info(lambdaXml.replace("\n", "").replace("  ", ""))
          lambdaXml
      }
    }
    val fs: FileSystem = getFileSystem(filePath)
    val fsc = fs.asInstanceOf[NdpHdfsFileSystem]
    val inFile = getFilePath(filePath)
    val inStrm = fsc.open(new Path(inFile), 4096, readParam).asInstanceOf[FSDataInputStream]
  }
}
