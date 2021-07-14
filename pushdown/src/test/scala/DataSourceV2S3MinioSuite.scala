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
 *  V2 data source, but using an S3 API accessing a minio server.
 *  The purpose of this suite is to make sure that the
 *  data source supports a known AWS S3 compliant server.
 */
class DataSourceV2S3MinioSuite extends DataSourceV2Suite {

  private val s3IpAddr = "minioserver:9000"

  /* For minio, we use an extended set of options,
   * which includes disabling pushdown for syntax it does not support.
   */
  override def sparkConf: SparkConf = super.sparkConf
      .set("spark.datasource.pushdown.endpoint", s"http://$s3IpAddr")
      .set("spark.datasource.pushdown.accessKey", System.getProperty("user.name"))
      .set("spark.datasource.pushdown.secretKey", "admin123")
      .set("spark.datasource.pushdown.DisableGroupbyPush", "")
      .set("spark.datasource.pushdown.DisableSupportsIsNull", "")
      .set("spark.datasource.pushdown.DisabledCasts", "NUMERIC")
      .set("spark.datasource.pushdown.DisableDistinct", "")
      .set("spark.datasource.pushdown.partitions", "1")

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
