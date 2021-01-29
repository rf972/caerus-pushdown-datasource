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

//package org.apache.spark.sql.jdbc
package com.github.datasource.test

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class DataSourceV2Suite extends QueryTest with SharedSparkSession {
  import testImplicits._
  private val s3IpAddr = "minioserver"
  override def sparkConf: SparkConf = super.sparkConf
      .set("spark.datasource.pushdown.endpoint", s"""http://$s3IpAddr:9000""")
      .set("spark.datasource.pushdown.accessKey", "admin")
      .set("spark.datasource.pushdown.secretKey", "admin123")

  private val schema = new StructType()
       .add("i",IntegerType,true)
       .add("j",IntegerType,true)
       .add("k",IntegerType,true)

  private def df() : DataFrame = {    
    spark.read
      .format("com.github.datasource")
      .schema(schema)
      .option("format", "csv")
      .load("s3a://spark-test/ints.tbl")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    df.createOrReplaceTempView("integers")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("simple scan") {
    checkAnswer(df, Seq(Row(0, 5, 1), Row(1, 10, 2), Row(2, 5, 1),
                        Row(3, 10, 2), Row(4, 5, 1), Row(5, 10, 2), Row(6, 5, 1)))
    df.show()    
  }
  test("simple project") {
    checkAnswer(df.select("i","j","k"),
                Seq(Row(0, 5, 1), Row(1, 10, 2), Row(2, 5, 1),
                       Row(3, 10, 2), Row(4, 5, 1), Row(5, 10, 2), Row(6, 5, 1)))
    checkAnswer(df.select("i","j"),
                Seq(Row(0, 5), Row(1, 10), Row(2, 5),
                       Row(3, 10), Row(4, 5), Row(5, 10), Row(6, 5)))
    checkAnswer(df.filter("i >= 5"),
                Seq(Row(5,10,2),Row(6,5,1)))
  }
  test("basic aggregate") {
    checkAnswer(df.filter("i > 4")
                  .agg(sum("i") * sum("j")),
                 Seq(Row(165)))
    checkAnswer(df.agg(sum("j")),
                Seq(Row(50)))
    checkAnswer(df.agg(min("k"), max("k")),
                Seq(Row(1, 2)))
    checkAnswer(df.filter("i > 4")
                  .agg(sum("j"), min("j"), max("j"), avg("j")),
                Seq(Row(15, 5, 10, 7.5)))
    checkAnswer(df.filter("i > 4")
                  .agg(sum("j"), min("j"), avg("j"), max("j")),
                Seq(Row(15, 5, 7.5, 10)))
    checkAnswer(df.agg(sum("i"), min("i"), max("i"), avg("i")),
                Seq(Row(21, 0, 6, 3.0)))
  }
  test("aggregate") {
    checkAnswer(sql("SELECT sum(i) FROM integers GROUP BY j"),
                Seq(Row(12), Row(9)))
    checkAnswer(df.groupBy("j").agg(sum("i")),
                Seq(Row(5, 12), Row(10, 9)))
    checkAnswer(sql("SELECT j, sum(i) FROM integers GROUP BY j"),
                Seq(Row(5, 12), Row(10, 9)))
    checkAnswer(sql("SELECT sum(i), j FROM integers GROUP BY j"),
                Seq(Row(12, 5), Row(9, 10)))
    checkAnswer(sql("SELECT sum(k), j, sum(i), min(k) FROM integers GROUP BY j"),
                Seq(Row(4, 5, 12, 1), Row(6, 10, 9, 2)))
    checkAnswer(sql("SELECT sum(i), j, sum(k), min(k) FROM integers WHERE i > 1" +
                                    " GROUP BY j"),
                Seq(Row(12, 5, 3, 1), Row(8, 10, 4, 2)))
    checkAnswer(sql("SELECT sum(i), j, sum(k), min(k) FROM integers WHERE i > 1" +
                                    " GROUP BY j"),
                Seq(Row(12, 5, 3, 1), Row(8, 10, 4, 2)))
  }
  test("aggregate multiple group by") {  
    checkAnswer(sql("SELECT k, sum(k * j), j, k FROM integers WHERE i > 1" +
                    " GROUP BY j, k"),
                Seq(Row(1, 15, 5, 1),Row(2, 40, 10, 2)))
  }

  test ("aggregate with expressions") {

    checkAnswer(sql("SELECT sum(k * j) FROM integers WHERE i > 1" +
                    " GROUP BY j"),
                Seq(Row(15),Row(40)))
    checkAnswer(sql("SELECT j, sum(k * j) FROM integers WHERE i > 1" +
                    " GROUP BY j"),
                Seq(Row(5, 15),Row(10, 40)))
    checkAnswer(sql("SELECT sum(k * j), j FROM integers WHERE i > 1" +
                    " GROUP BY j"),
                Seq(Row(15, 5),Row(40, 10)))
    checkAnswer(sql("SELECT j, sum(i * k) FROM integers WHERE i != 6" +
                    " GROUP BY j"),
                Seq(Row(5, 6),Row(10, 18)))
    checkAnswer(sql("SELECT sum(k + j) FROM integers WHERE i > 1" +
                    " GROUP BY j"),
                Seq(Row(18),Row(24)))
  }
}