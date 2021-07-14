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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

/** Is a suite of testing which exercises the
 *  V2 data source, but using an S3 API against the NDP server.
 */
class DataSourceV2S3NdpSuite extends DataSourceV2Suite {

  private val s3IpAddr = "dikehdfs:9858"
  override def sparkConf: SparkConf = super.sparkConf
      .set("spark.datasource.pushdown.endpoint", s"http://$s3IpAddr")
      .set("spark.datasource.pushdown.accessKey", System.getProperty("user.name"))
      .set("spark.datasource.pushdown.secretKey", "admin123")

  override protected def df() : DataFrame = {
    spark.read
      .format("com.github.datasource")
      .schema(schema)
      .option("format", "csv")
      .option("header", "true")
      .load("s3a://unit-test-csv/")
  }
  override protected def dfNoHeader() : DataFrame = {
    spark.read
      .format("pushdown")
      .schema(schema)
      .option("format", "csv")
      .option("header", "false")
      .load("s3a://unit-test-csv-noheader/")
  }
}
