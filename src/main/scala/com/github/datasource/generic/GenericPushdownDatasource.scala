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

import java.util
import java.util.HashMap

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, SupportsRead,
  Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Creates a data source object for Spark that
 *  supports pushdown of predicates such as Filter, Project and Aggregate.
 *
 */
class GenericPushdownDatasource extends TableProvider
  with SessionConfigSupport with DataSourceRegister {

  private val logger = LoggerFactory.getLogger(getClass)
  override def toString: String = s"GenericPushdownDataSource()"
  override def supportsExternalMetadata(): Boolean = true
  // GenericPushdownDatasource.checkInitialized

  private val sparkSession: SparkSession = SparkSession
    .builder()
    .getOrCreate()

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (options.get("format") == "parquet") {
      /* With parquet, we infer the schema from the metadata.
       */
      val file = NdpApi.extractFilename(options.get("path"))
      val server = NdpApi.extractServer(options.get("path"))
      logger.info(s"getting schema for: ${server}/${file}")
      NdpApi.getSchema(file, server)
    } else {
      /* Other types like CSV require a user-supplied schema */
      throw new IllegalArgumentException("requires a user-supplied schema")
    }
  }
  override def getTable(schema: StructType,
                        transforms: Array[Transform],
                        options: util.Map[String, String]): Table = {
    logger.trace("getTable: Options " + options)
    new GenericPushdownBatchTable(schema, options)
  }

  override def keyPrefix(): String = {
    "genericPushdown"
  }
  override def shortName(): String = "genericPushdown"
}

object GenericPushdownDatasource {
  private val logger = LoggerFactory.getLogger(getClass)
  var initialized = false
  private val sparkSession: SparkSession = SparkSession
    .builder()
    .getOrCreate()
  def checkInitialized(): Unit = {
    if (!initialized) {
      initialized = true
      logger.info("Adding new GenericPushdowntimization Rule")
      sparkSession.experimental.extraOptimizations ++= Seq(GenericPushdownOptimizationRule)
    }
  }
  checkInitialized()
}
/** Creates a Table object that supports pushdown predicates
 *   such as Filter, Project, and Aggregate.
 *
 * @param schema the StructType format of this table
 * @param options the parameters for creating the table
 *                "endpoint" is the server name,
 *                "accessKey" and "secretKey" are the credentials for above server.
 *                 "path" is the full path to the s3 file.
 */
class GenericPushdownBatchTable(schema: StructType,
                           options: util.Map[String, String])
  extends Table with SupportsRead {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def name(): String = this.getClass.toString

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(params: CaseInsensitiveStringMap): ScanBuilder =
    new GenericPushdownScanBuilder(schema, options)
}

/** Creates a builder for scan objects.
 *  For s3 we build the S3Scan, and for hdfs HdfsScan.
 *
 * @param schema the format of the columns
 * @param options the options (see PushdownBatchTable for full list.)
 */
class GenericPushdownScanBuilder(schema: StructType,
                                 options: util.Map[String, String])
  extends ScanBuilder {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Returns a scan object for this particular query.
   *   Currently we only support S3 and Hdfs.
   *
   * @return the scan object either a S3Scan or HdfsScan
   */
  override def build(): Scan = {
    /* Make the map modifiable.
     * The objects below can override defaults.
     */
    val opt: util.Map[String, String] = new HashMap[String, String](options)
    if (!options.get("path").contains("hdfs")) {
      throw new Exception(s"endpoint ${options.get("endpoint")} is unexpected")
    }
    new GenericPushdownScan(schema, opt)
  }
}
