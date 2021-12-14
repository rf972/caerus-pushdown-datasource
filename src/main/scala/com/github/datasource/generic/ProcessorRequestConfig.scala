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

import java.io.StringReader
import java.io.StringWriter
import java.util.Iterator
import javax.json.Json
import javax.json.JsonArrayBuilder
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import javax.json.JsonReader
import javax.json.JsonWriter
import javax.xml.namespace.QName
import javax.xml.stream._

import scala.xml._

import com.github.datasource.hdfs.HdfsPartition
import org.slf4j.LoggerFactory

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition

/**
 *
 * @param url
 * @param rowGroup
 * @param query
 */
class ProcessorRequestConfig(query: String,
                             url: String = ProcessorRequestConfig.fileTag,
                             rowGroup: String = ProcessorRequestConfig.rowGroupTag,
                             useNdp: String = "True") {
  private val logger = LoggerFactory.getLogger(getClass)
  def configString: String = {
    val configBuilder = Json.createObjectBuilder()
    configBuilder.add("url", url)
    configBuilder.add("query", query)
    configBuilder.add("row_group", rowGroup)
    configBuilder.add("use_ndp", useNdp)

    // For now we will assume simple pipe with ordered connections
    val config = configBuilder.build()

    val stringWriter = new StringWriter()
    val writer = Json.createWriter(stringWriter)
    writer.writeObject(config)
    writer.close()

    val jsonString = stringWriter.getBuffer().toString()
    val reader = Json.createReader(new StringReader(jsonString))
    val jsonObject = reader.readObject()

    val stringWriter2 = new StringWriter()
    val writer2 = Json.createWriter(stringWriter2)
    writer2.writeObject(jsonObject)
    writer2.close()
    val jsonString2 = stringWriter2.getBuffer().toString()
    jsonString2
  }
}

object ProcessorRequestConfig {

  def apply(query: String,
            url: String = ProcessorRequestConfig.fileTag,
            rowGroup: String = ProcessorRequestConfig.rowGroupTag): ProcessorRequestConfig =
    new ProcessorRequestConfig(query, url, rowGroup)
  // We set the file to this value in the completed DAG.
  // This allows us to replace this string with the actual file,
  // when we want to use or re-use the dag for different files.
  private val fileTag = "FILE_TAG"
  private val rowGroupTag = "ROW_GROUP_TAG"
  private val tableTag = "TABLE_TAG"
  def configString(config: String, url: String): String = {
    config.replace(fileTag, url)
  }
  def configString(config: String, part: HdfsPartition, user: String): String = {
    val newUrl = s"${part.name}?op=SELECTCONTENT&user.name=${user}"
    config.replace(fileTag, newUrl)
          .replace(rowGroupTag, part.index.toString)
          .replace(tableTag, "arrow")
  }
}
