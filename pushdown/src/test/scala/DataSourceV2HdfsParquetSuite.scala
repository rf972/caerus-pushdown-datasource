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

package com.github.datasource.test

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.DataFrame

/** Is a suite of testing which exercises the
 *  V2 data source, but using an HDFS API,
 *  and a parquet file, but without NDP.
 */
class DataSourceV2HdfsParquetSuite extends DataSourceV2Suite {

  var host = "dikehdfs"
  /** Returns the dataframe for the sample data
   *  read in through the ndp data source.
   */
  override protected def df(): DataFrame = {
    spark.read
      .format("pushdown")
      .option("header", "true")
      .option("format", "parquet")
      .option("DisableAggregatePush", "")
      .load(s"hdfs://${host}/unit-test-parquet/")
  }
  override protected def dfNoHeader(): DataFrame = {
     spark.read
      .format("com.github.datasource")
      .option("header", "true")
      .option("format", "parquet")
      .option("DisableAggregatePush", "")
      .load(s"hdfs://${host}/unit-test-parquet/")
  }
}
