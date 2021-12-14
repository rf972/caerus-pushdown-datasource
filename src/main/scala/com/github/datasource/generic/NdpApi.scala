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

import java.io._
import java.net._
import java.net.HttpURLConnection
import javax.json.Json
import javax.json.JsonObject

import org.slf4j.LoggerFactory

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

object NdpApi {
  private val logger = LoggerFactory.getLogger(getClass)

  def getStatus(file: String, server: String): JsonObject = {
    val url = "http://" + server + "/" + file +
              "?op=GETNDPINFO&user.name=" + System.getProperty("user.name")
    val connection: HttpURLConnection =
       new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    val reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))
    val jsonReader = Json.createReader(reader)
    val jsonObj = jsonReader.readObject()
    jsonReader.close
    reader.close
    jsonObj
  }
  def getFileStatus(fileName: String, server: String): JsonObject = {
    val fileCurated = fileName.replace("//", "/")
    val url = "http://" + server + "/webhdfs/v1/" + fileCurated +
      "?op=GETFILESTATUS&user.name=" + System.getProperty("user.name")
    val connection: HttpURLConnection =
      new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    val reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))
    val jsonReader = Json.createReader(reader)
    val jsonObj = jsonReader.readObject()
    jsonReader.close
    reader.close
    jsonObj
  }
  def getListStatus(fileName: String, server: String): JsonObject = {
    val fileCurated = fileName.replace("//", "/")
    val url = "http://" + server + "/webhdfs/v1/" + fileCurated +
      "?op=LISTSTATUS&user.name=" + System.getProperty("user.name")
    val connection: HttpURLConnection =
      new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    val reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))
    val jsonReader = Json.createReader(reader)
    val jsonObj = jsonReader.readObject()
    jsonReader.close
    reader.close
    jsonObj
  }
  /** Returns a list of file names represented by an input file or directory.
   *
   * @param fileName the full filename path
   * @return Map[String, BlockLocation] The Key is the filename
   *                     the value is the Array of BlockLocation
   */
  def getFileList(fileName: String, server: String):
                  Seq[String] = {
    val fileStatus = getFileStatus(fileName, server)
    var fileArray: Array[String] = Array[String]()
    val fileType = fileStatus.getJsonObject("FileStatus").getString("type")
    if (fileType == "FILE") {
      // Use MaxValue to indicate we want info on all blocks.
      fileArray = fileArray ++ Array(fileName)
    } else {
      /* fileToRead is a directory. So get the contents of this directory.
       * For each file in the directory create a new map entry with filename as key.
       */
      val status = getListStatus(fileName, server)
      val fileStatusArray = status.getJsonObject("FileStatuses").getJsonArray("FileStatus")
      for (i <- 0 until fileStatusArray.size()) {
        val item = fileStatusArray.getJsonObject(i)
        if (item.getString("type") == "FILE" && (item.getString("pathSuffix").contains(".csv") ||
          item.getString("pathSuffix").contains(".tbl") ||
          item.getString("pathSuffix").contains(".parquet"))) {
          val currentFile = item.getString("pathSuffix").toString
          fileArray = fileArray ++ Array(currentFile)
        }
      }
    }
    fileArray.toSeq
  }
  def getSchema(path: String, server: String = ""): StructType = {
    val fileStatusArray = getFileList(path, server)
    val fileName = fileStatusArray(0)
    val ndpStatusJson = NdpApi.getStatus(s"${path}/${fileName}", server)

    var schema = new StructType()
    val dtypes = ndpStatusJson.getJsonArray("dtypes")
    val columns = ndpStatusJson.getJsonArray("columns")
    for (i <- 0 until dtypes.size()) {
      val dataType = dtypes.getString(i) match {
        case "int64" => LongType
        case "float64" => DoubleType
        case "object" => StringType
      }
      schema = schema.add(columns.getString(i), dataType, false)
    }
    schema
  }
  def extractFilename(path: String): String = path.replaceFirst("hdfs://.*:9860/", "")
  def extractServer(path: String): String = path.replaceFirst("hdfs://", "")
    .replaceFirst(":9860.*", ":9860")
}
