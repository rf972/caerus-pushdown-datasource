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

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.github.datasource.common.Pushdown
import com.github.datasource.parse.{ParquetRowIterator, RowIteratorFactory}
import org.apache.commons.csv._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
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
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.{Aggregation, Filter}
import org.apache.spark.sql.types._

/** A Factory to fetch the correct type of
 *  store object.
 */
object HdfsStoreFactory {
  /** Returns the store object.
   *
   * @param pushdown object handling filter, project and aggregate pushdown
   * @param options the parameters including those to construct the store
   * @return a new HdfsStore object constructed with above parameters.
   */
  def getStore(pushdown: Pushdown,
               options: java.util.Map[String, String],
               sparkSession: SparkSession,
               sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration]):
      HdfsStore = {
    new HdfsStore(pushdown, options, sparkSession, sharedConf)
  }
}
/** A hdfs store object which can connect
 *  to a file on hdfs filesystem, specified by options("path"),
 *  And which can read a partition with any of various pushdowns.
 *
 * @param pushdown object handling filter, project and aggregate pushdown
 * @param options the parameters including those to construct the store
 */
class HdfsStore(pushdown: Pushdown,
                options: java.util.Map[String, String],
                sparkSession: SparkSession,
                sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration]) {
  override def toString() : String = "HdfsStore" + options + pushdown.filters.mkString(", ")
  protected val path = options.get("path")
  protected val isPushdownNeeded: Boolean = {
    /* Determines if we should send the pushdown to ndp.
     * If any of the pushdowns are in use (project, filter, aggregate),
     * then we will consider that pushdown is needed.
     */
    (true || /* FIXME: added for csv/parquet forcing */
     (pushdown.prunedSchema.length != pushdown.schema.length) ||
     (pushdown.filters.length > 0) ||
     (pushdown.aggregation.aggregateExpressions.length > 0) ||
     (pushdown.aggregation.groupByExpressions.length > 0))
  }
  protected val endpoint = {
    val server = path.split("/")(2)
    if (path.contains("ndphdfs://")) {
      ("ndphdfs://" + server + ":9860")
    } else if (path.contains("webhdfs://")) {
      ("webhdfs://" + server + ":9870")
    } else {
      ("hdfs://" + server + ":9000")
    }
  }
  val filePath = {
    val server = path.split("/")(2)
    if (path.contains("ndphdfs://")) {
      val str = path.replace("ndphdfs://" + server, "ndphdfs://" + server + ":9860")
      str
    } else if (path.contains("webhdfs")) {
      path.replace(server, server + ":9870")
    } else {
      path.replace(server, server + ":9000")
    }
  }
  protected val logger = LoggerFactory.getLogger(getClass)
  protected val configure: Configuration = {
    val conf = new Configuration()
    conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    conf.set("dfs.client.cache.readahead", "0")
    conf.set("fs.ndphdfs.impl", classOf[org.dike.hdfs.NdpHdfsFileSystem].getName)
    conf
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
  protected val fileSystemType = fileSystem.getScheme

  /** Returns a reader for a given Hdfs partition.
   *  Determines the correct start offset by looking backwards
   *  to find the end of the prior line.
   *  Helps in cases where the last line of the prior partition
   *  was split on the partition boundary.  In that case, the
   *  prior partition's last (incomplete) is included in the next partition.
   *
   * @param partition the partition to read
   * @return a new BufferedReader for this partition.
   */
  def getReader(partition: HdfsPartition,
                startOffset: Long = 0, length: Long = 0): BufferedReader = {
    val filePath = new Path(partition.name)
    val (readParam, query) = {
      if (!isPushdownNeeded ||
          fileSystemType != "ndphdfs") {
        ("", "")
      } else {
        val (requestQuery, requestSchema) = {
          if (fileSystemType == "ndphdfs") {
            logger.info("SQL Query (readable): " + pushdown.getReadableQuery(partition))
            (pushdown.queryFromSchema(partition),
            Pushdown.schemaString(pushdown.schema))
          } else {
            ("", "")
          }
        }
        options.get("format") match {
          case "parquet" => (new ProcessorRequestParquet(partition.modifiedTime, partition.index,
                                 requestQuery, partition.length,
                                 headerType).toXml, requestQuery)
          case _ => (new ProcessorRequest(requestSchema, requestQuery, partition.length,
                         headerType).toXml, requestQuery)
        }
      }
    }
    logger.info(readParam)
    /* When we are targeting ndphdfs, but we do not have a pushdown,
     * we will not pass the processor element.
     * This allows the NDP server to optimize further.
     */
    if (isPushdownNeeded &&
        fileSystemType == "ndphdfs") {
        logger.info(s"SQL Query partition: ${partition.toString}")
        logger.info(s"SQL Query: ${query}")
        val fs = fileSystem.asInstanceOf[NdpHdfsFileSystem]
        val inStrm = fs.open(filePath, 4096, readParam).asInstanceOf[FSDataInputStream]
        inStrm.seek(partition.offset)
        new BufferedReader(new InputStreamReader(inStrm))
    } else {
        val inStrm = fileSystem.open(filePath)
        if (fileSystemType == "ndphdfs") {
          logger.info(s"No Pushdown to ${fileSystemType} partition: ${partition.toString}")
        }
        inStrm.seek(startOffset)
        new BufferedReader(new InputStreamReader(new BoundedInputStream(inStrm, length)))
    }
  }

  def getDataType(primitiveType: PrimitiveTypeName): DataType = {
    primitiveType match {
        case BOOLEAN => BooleanType
        case INT32 => IntegerType
        case INT64 => LongType
        case DOUBLE => DoubleType
        case FLOAT => FloatType
        case _ => StringType
    }
  }
  def getDataTypes(parquetMetadata: FileMetaData): Array[DataType] = {
    val schema = parquetMetadata.getSchema()
    val types: java.util.List[Type] = schema.asGroupType().getFields()
    var dataTypes = new Array[DataType](types.size)
    for (i <- 0 to types.size - 1) {
      val pType = types.get(i).asPrimitiveType()
      dataTypes(i) = getDataType(pType.getPrimitiveTypeName())
    }
    dataTypes
  }
  def getParquetIteratorOld(partition: HdfsPartition): Iterator[InternalRow] = {
    // sparkSchema = new ParquetToSparkSchemaConverter(config).convert(requestedSchema)
    // List<ColumnDescriptor> columns = requestedSchema.getColumns();
    logger.info(s"get reader ${partition.name}")
    // var hadoopConf: Configuration =
    //        sparkSession.sessionState.newHadoopConf() // WithOptions(options)
    val hadoopConf = sharedConf.value.value
    // hadoopConf.set("io.file.buffer.size", "16777216")
    val readOptions = HadoopReadOptions.builder(hadoopConf)
                                       .build()
    // logger.info(hadoopConf.getPropsWithPrefix("hadoop").toString)
    // logger.info(hadoopConf.getPropsWithPrefix("io").toString)
    // Don't forget to pass hadoop Config through
    // val reader = ParquetFileReader.open(NdpInputFile.fromPath(partition.name), readOptions)
    val reader = ParquetFileReader.open(
                   HadoopInputFile.fromPath(new Path(partition.name), hadoopConf), readOptions)
    val schema = reader.getFileMetaData().getSchema()
    val types: java.util.List[Type] = schema.asGroupType().getFields()
    val dataTypes = getDataTypes(reader.getFileMetaData())
    ParquetRowIterator(reader, dataTypes)
  }
  def getParquetIterator(partition: HdfsPartition): Iterator[InternalRow] = {

    val hadoopConf = sharedConf.value.value
    logger.info(s"get reader ${partition.name}")
    val readSupport = new ParquetReadSupport(
      None,
      false,
      LegacyBehaviorPolicy.CORRECTED,
      LegacyBehaviorPolicy.LEGACY)
      hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      pushdown.schema.json)
    val reader = new ParquetRecordReader[InternalRow](readSupport)
    val iter = new RecordReaderIterator[InternalRow](reader)
    // taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext =
        new TaskAttemptContextImpl(sharedConf.value.value, attemptId)
    val filePath = new Path(new URI(partition.name))
    val split = new FileSplit(filePath, 0, partition.length, Array.empty[String])
    reader.initialize(split, hadoopAttemptContext)
    /* val attrib = pushdown.schema.map(f => AttributeReference(f.name,
                                           f.dataType, f.nullable, f.metadata)())
    val fullSchema = attrib
    val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
    */
    iter
    // iter.map(unsafeProjection)
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
    val fileToRead = new Path(fileName)
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
    val fileToRead = new Path(filePath)
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
  /** Returns the offset, length in bytes of an hdfs partition.
   *  This takes into account any prior lines that might be incomplete
   *  from the prior partition.
   *
   * @param partition the partition to find start for
   * @return (offset, length) - Offset to begin reading partition, Length of partition.
   */
  @throws(classOf[Exception])
  def getPartitionInfo(partition: HdfsPartition) : (Long, Long) = {
    if (fileSystemType == "ndphdfs" &&
        isPushdownNeeded) {
      // No need to find offset, ndp server does this under the covers for us.
      // When Processor is disabled, we need to deal with partial lines for ourselves.
      return (partition.offset, partition.length)
    }
    val currentPath = new Path(partition.name)
    var startOffset = partition.offset
    var nextChar: Integer = 0
    if (partition.offset != 0) {
      /* Scan until we hit a newline. This skips the (normally) partial line,
       * which the prior partition will read, and guarantees we get a full line.
       * The only way to guarantee full lines is by reading up to the line terminator.
       */
      val inputStream = fileSystem.open(currentPath)
      inputStream.seek(partition.offset)
      val reader = new BufferedReader(new InputStreamReader(inputStream))
      do {
        nextChar = reader.read
        startOffset += 1
      } while ((nextChar.toChar != '\n') && (nextChar != -1));
    }
    var partitionLength = (partition.offset + partition.length) - startOffset
    /* Scan up to the next line after the end of the partition.
     * We always include this next line to ensure we are reading full lines.
     * The only way to guarantee full lines is by reading up to the line terminator.
     */
    val inputStream = fileSystem.open(currentPath)
    inputStream.seek(partition.offset + partition.length)
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    do {
      nextChar = reader.read
      // Only count the char if we are not at end of line.
      if (nextChar != -1) {
        partitionLength += 1
      }
    } while ((nextChar.toChar != '\n') && (nextChar != -1));
    // println(s"partition: ${partition.index} offset: ${startOffset} length: ${partitionLength}")
    (startOffset, partitionLength)
  }
  /** The kind of header we should use.
   *  In our case we only ever use None or Ignore, since
   *  we use casts and column numbers, so the header is not needed.
   * @return NONE or IGNORE
   */
  def headerType(): String = {
    /* If we do not push down, then we will read from hdfs directly,
     * and therefor need to skip the header ourselves.
     * When we use ndp, it skips the header for us since we tell it about the header.
     */
    if (options.containsKey("header") && (options.get("header") == "true")) {
      "IGNORE"
    } else {
      "NONE"
    }
  }
  /** Returns true if we should skip the header.
   *
   * @param partition the partition to read
   * @return true to skip the header and false otherwise.
   */
  def skipHeader(partition: HdfsPartition): Boolean = {
    /* If we do not push down, then we will read from hdfs directly,
     * and therefor need to skip the header ourselves.
     * When we use ndp, it skips the header for us since we tell it about the header.
     */
    if (options.containsKey("header") && (options.get("header") == "true")) {
      (partition.index == 0 && !isPushdownNeeded)
    } else {
      false
    }
  }
  /** Returns an Iterator over InternalRow for a given Hdfs partition.
   *
   * @param partition the partition to read
   * @return a new CsvRowIterator for this partition.
   */
  def getRowIter(partition: HdfsPartition): Iterator[InternalRow] = {
    val (offset, length) = getPartitionInfo(partition)
    RowIteratorFactory.getIterator(getReader(partition, offset, length),
                                   pushdown.readSchema,
                                   options.get("format"),
                                   skipHeader(partition))
  }
  /** Returns an Iterator over InternalRow for a given Hdfs partition.
   *
   * @param partition the partition to read
   * @return a new CsvRowIterator for this partition.
   */
  def getRowIterParquet(partition: HdfsPartition): Iterator[InternalRow] = {
    RowIteratorFactory.getIterator(getReader(partition, 0, 0),
                                   pushdown.readSchema,
                                   "csv",
                                   skipHeader(partition))
  }
}

/** Related routines for the HDFS connector.
 *
 */
object HdfsStore {

  /** Returns true if pushdown is supported by this flavor of
   *  filesystem represented by a string of "filesystem://filename".
   *
   * @param options map containing "path".
   * @return true if pushdown supported, false otherwise.
   */
  def pushdownSupported(options: util.Map[String, String]): Boolean = {
    if (options.get("path").contains("ndphdfs://")) {
      true
    } else {
      // other filesystems like hdfs and webhdfs do not support pushdown.
      false
    }
  }
}
