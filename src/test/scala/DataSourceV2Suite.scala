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
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/** Is a test suite for the V2 datasource.
 *  This test is regardless of API so that this class can be
 *  extended with an overloaded df method to allow multiple types of
 *  configurations to use the same tests.
 *
 */
abstract class DataSourceV2Suite extends QueryTest with SharedSparkSession {
  import testImplicits._

  protected val schema = new StructType()
       .add("i", IntegerType, true)
       .add("j", IntegerType, true)
       .add("k", IntegerType, true)

  /** returns a dataframe object, which is to be used for testing of
   *  each test case in this suite.
   *  This can be overloaded in a new suite, which defines
   *  its own data frame.
   *
   * @return DataFrame - The new dataframe object to be used in testing.
   */
  protected def df() : DataFrame

  /** Initializes a data frame with the sample data and
   *  then writes this dataframe out to hdfs.
   */
  protected def initData(): Unit = {
    val s = spark
    import s.implicits._
    val testDF = dataValues.toSeq.toDF("i", "j", "k")
    /* Write a CSV file both with and without header.
     */
    testDF.select("*").repartition(1)
      .write.mode("overwrite")
      .option("delimiter", ",")
      .format("csv")
      .option("header", "true")
      .option("partitions", "1")
      .save("hdfs://dikehdfs:9000/unit-test-csv")
    testDF.select("*").repartition(1)
      .write.mode("overwrite")
      .option("delimiter", ",")
      .format("csv")
      .option("header", "false")
      .option("partitions", "1")
      .save("hdfs://dikehdfs:9000/unit-test-csv-noheader")
    /* Write Parquet file. */
    testDF.select("*").repartition(1)
      .write.mode("overwrite")
      .format("parquet")
      .option("partitions", "1")
      .save("hdfs://dikehdfs:9000/unit-test-parquet")
  }
  private val dataValuesInt = Seq((0, 5, 1), (1, 10, 2), (2, 5, 1),
                                  (3, 10, 2), (4, 5, 1), (5, 10, 2), (6, 5, 1))
  // Using long currently for NDP binary mode, which does not support int yet.
  private val dataValues = Seq((0L, 5L, 1L), (1L, 10L, 2L), (2L, 5L, 1L),
                               (3L, 10L, 2L), (4L, 5L, 1L), (5L, 10L, 2L), (6L, 5L, 1L))
  /** returns a dataframe object, which is to be used for testing of
   *  each test case in this suite.
   *  In this case, the data for the DataFrame does not contain a header.
   *  This can be overloaded in a new suite, which defines
   *  its own data frame.
   *
   * @return DataFrame - The new dataframe object to be used in testing.
   */
  protected def dfNoHeader() : DataFrame

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
    // spark.sparkContext.setLogLevel("INFO")
    // spark.sparkContext.setLogLevel("TRACE")
    initData()
    df.createOrReplaceTempView("integers")
    dfNoHeader.createOrReplaceTempView("integersNoHeader")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
  test("simple scan") {
    checkAnswer(df, Seq(Row(0, 5, 1), Row(1, 10, 2), Row(2, 5, 1),
                        Row(3, 10, 2), Row(4, 5, 1), Row(5, 10, 2), Row(6, 5, 1)))
    checkAnswer(dfNoHeader, Seq(Row(0, 5, 1), Row(1, 10, 2), Row(2, 5, 1),
                            Row(3, 10, 2), Row(4, 5, 1), Row(5, 10, 2), Row(6, 5, 1)))
    df.show()
    dfNoHeader.show()
  }
  test("simple project") {
    checkAnswer(df.select("i", "j", "k"),
                Seq(Row(0, 5, 1), Row(1, 10, 2), Row(2, 5, 1),
                       Row(3, 10, 2), Row(4, 5, 1), Row(5, 10, 2), Row(6, 5, 1)))
    checkAnswer(df.select("i", "j"),
                Seq(Row(0, 5), Row(1, 10), Row(2, 5),
                       Row(3, 10), Row(4, 5), Row(5, 10), Row(6, 5)))
    checkAnswer(df.filter("i >= 5"),
                Seq(Row(5, 10, 2), Row(6, 5, 1)))
  }
  test("no header") {
    checkAnswer(dfNoHeader.select("i", "j", "k"),
                Seq(Row(0, 5, 1), Row(1, 10, 2), Row(2, 5, 1),
                       Row(3, 10, 2), Row(4, 5, 1), Row(5, 10, 2), Row(6, 5, 1)))
    checkAnswer(dfNoHeader.select("i", "j"),
                Seq(Row(0, 5), Row(1, 10), Row(2, 5),
                       Row(3, 10), Row(4, 5), Row(5, 10), Row(6, 5)))
    checkAnswer(dfNoHeader.filter("i >= 5"),
                Seq(Row(5, 10, 2), Row(6, 5, 1)))
    checkAnswer(dfNoHeader.filter("i > 4")
                  .agg(sum("i") * sum("j")),
                Seq(Row(165)))
    checkAnswer(dfNoHeader.agg(sum("j")),
                Seq(Row(50)))
    checkAnswer(dfNoHeader.agg(min("k"), max("k")),
                Seq(Row(1, 2)))
    checkAnswer(sql("SELECT count(*) FROM integersNoHeader"),
                Seq(Row(7)))
    checkAnswer(sql("SELECT sum(k + j) FROM integersNoHeader WHERE i > 1" +
                    " GROUP BY j"),
                Seq(Row(18), Row(24)))
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
                  .agg(sum("j"), min("j"), max("j")),
                Seq(Row(15, 5, 10)))
    checkAnswer(df.agg(sum("i"), min("i"), max("i")),
                Seq(Row(21, 0, 6)))
  }
  test("aggregate groupby") {
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
  test("aggregate distinct") {
    checkAnswer(sql("SELECT SUM(j) FROM integers WHERE i > 0"), Seq(Row(45)))
    checkAnswer(sql("SELECT SUM(DISTINCT j) FROM integers WHERE i > 0"), Seq(Row(15)))
    checkAnswer(sql("SELECT CAST(AVG(j) AS DECIMAL(5,2)) FROM integers"),
                Seq(Row(7.14))) // 7.14285714285714
    checkAnswer(sql("SELECT AVG(DISTINCT j) FROM integers"),
                Seq(Row(7.5)))
  }
  test("aggregate multiple group by") {
    checkAnswer(sql("SELECT k, sum(k * j), j, k FROM integers WHERE i > 1" +
                    " GROUP BY j, k"),
                Seq(Row(1, 15, 5, 1), Row(2, 40, 10, 2)))
  }
  test ("aggregate with expressions") {

    checkAnswer(sql("SELECT sum(k * j) AS prod_kj FROM integers WHERE i > 1" +
                    " GROUP BY j"),
                Seq(Row(15), Row(40)))
    checkAnswer(sql("SELECT j, sum(k * j) FROM integers WHERE i > 1" +
                    " GROUP BY j"),
                Seq(Row(5, 15), Row(10, 40)))
    checkAnswer(sql("SELECT sum(k * j), j FROM integers WHERE i > 1" +
                    " GROUP BY j"),
                Seq(Row(15, 5), Row(40, 10)))
    checkAnswer(sql("SELECT j, sum(i * k) FROM integers WHERE i != 6" +
                    " GROUP BY j"),
                Seq(Row(5, 6), Row(10, 18)))
    checkAnswer(sql("SELECT sum(k + j) FROM integers WHERE i > 1" +
                    " GROUP BY j"),
                Seq(Row(18), Row(24)))
  }
  test ("aggregate count") {
    checkAnswer(sql("SELECT count(*) FROM integers"),
                Seq(Row(7)))
    checkAnswer(sql("SELECT count(i) FROM integers"),
                Seq(Row(7)))
    checkAnswer(sql("SELECT count(i) as count_of_i FROM integers"),
                Seq(Row(7)))
    checkAnswer(sql("SELECT count(j) FROM integers"),
                Seq(Row(7)))
    checkAnswer(sql("SELECT count(k) FROM integers WHERE j = 5"),
                Seq(Row(4)))
    checkAnswer(sql("SELECT count(i) FROM integers WHERE j = 15"),
                Seq(Row(0)))
    checkAnswer(sql("SELECT count(k) FROM integers WHERE k = 2"),
                Seq(Row(3)))
    checkAnswer(sql("SELECT count(k) FROM integers WHERE k = 2 OR j=5"),
                Seq(Row(7)))
  }
  test ("aggregate count group by") {
    checkAnswer(sql("SELECT count(j) FROM integers" +
                    " GROUP BY j"),
                Seq(Row(3), Row(4)))
    checkAnswer(sql("SELECT count(j), k FROM integers" +
                    " GROUP BY k"),
                Seq(Row(4, 1), Row(3, 2)))
    checkAnswer(sql("SELECT k, count(j) FROM integers" +
                    " GROUP BY k"),
                Seq(Row(1, 4), Row(2, 3)))
    checkAnswer(sql("SELECT count(j), count(i) FROM integers" +
                    " GROUP BY j"),
                Seq(Row(4, 4), Row(3, 3)))
    checkAnswer(sql("SELECT j, count(j), count(i) FROM integers" +
                    " GROUP BY j"),
                Seq(Row(5, 4, 4), Row(10, 3, 3)))
    checkAnswer(df.groupBy("k").agg(count("j")),
                Seq(Row(1, 4), Row(2, 3)))
    checkAnswer(df.groupBy("k").agg(count("j"), count("i")),
                Seq(Row(1, 4, 4), Row(2, 3, 3)))
  }
  test ("aggregate count distinct") {
    checkAnswer(sql("SELECT count(DISTINCT i) FROM integers"),
                Seq(Row(7)))
    checkAnswer(sql("SELECT count(DISTINCT j) FROM integers"),
                Seq(Row(2)))
    checkAnswer(sql("SELECT count(DISTINCT k) FROM integers"),
                Seq(Row(2)))
    checkAnswer(sql("SELECT k, count(DISTINCT j) FROM integers" +
                    " GROUP BY k"),
                Seq(Row(2, 1), Row(1, 1)))
    checkAnswer(sql("SELECT count(DISTINCT j), k FROM integers" +
                    " GROUP BY k"),
                Seq(Row(1, 2), Row(1, 1)))
    checkAnswer(df.agg(countDistinct("j")),
                Seq(Row(2)))
    checkAnswer(df.agg(countDistinct("i")),
                Seq(Row(7)))
  }
}
// testOnly com.github.datasource.test.DataSourceV2HdfsSuite
// testOnly com.github.datasource.test.DataSourceV2S3Suite
// testOnly com.github.datasource.test.DataSourceV2HdfsSuite -- -z "no header"
