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

import java.io.StringReader
import java.io.StringWriter
import java.util.Iterator
import javax.json.Json
import javax.json.JsonArrayBuilder
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import javax.json.JsonWriter
import javax.xml.namespace.QName
import javax.xml.stream._

import scala.xml._

import org.slf4j.LoggerFactory

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition

/**
 *
 * @param fileName
 * @param compressionType
 * @param compressionLevel
 * @param nodes
 * @param filters
 * @param aggregate
 * @param test
 */
 class ProcessorRequestDag(fileName: String = ProcessorRequestDag.fileTag,
                           nodes: Array[String],
                           test: String = "",
                           compressionType: String = "ZSTD",
                           compressionLevel: String = "-100") {

    private val logger = LoggerFactory.getLogger(getClass)
    def dagString: String = {
        val dagBuilder = Json.createObjectBuilder()
        dagBuilder.add("Name", "DAG Projection")

        val inputNodeBuilder = Json.createObjectBuilder()
        inputNodeBuilder.add("Name", "InputNode")
        inputNodeBuilder.add("Type", "_INPUT")
        inputNodeBuilder.add("File", fileName)

        val outputNodeBuilder = Json.createObjectBuilder()
        outputNodeBuilder.add("Name", "OutputNode")
        outputNodeBuilder.add("Type", "_OUTPUT")
        outputNodeBuilder.add("CompressionType", compressionType);
        outputNodeBuilder.add("CompressionLevel", compressionLevel);

        val nodeArrayBuilder = Json.createArrayBuilder()
        nodeArrayBuilder.add(inputNodeBuilder.build())

        for (node <- nodes) {
          val nodeJsonObject = Json.createReader(new StringReader(node)).readObject()
          nodeArrayBuilder.add(nodeJsonObject)
        }
        nodeArrayBuilder.add(outputNodeBuilder.build())
        dagBuilder.add("NodeArray", nodeArrayBuilder)

        // For now we will assume simple pipe with ordered connections
        val dag = dagBuilder.build()

        val stringWriter = new StringWriter()
        val writer = Json.createWriter(stringWriter)
        writer.writeObject(dag)
        writer.close()

        stringWriter.getBuffer().toString()
    }
}

object ProcessorRequestDag {

  def apply(fileName: String,
            nodes: Array[String],
            test: String = "",
            compressionType: String = "ZSTD",
            compressionLevel: String = "-100"): ProcessorRequestDag =
    new ProcessorRequestDag(fileName, nodes, test,
                            compressionType, compressionLevel)
  // We set the file to this value in the completed DAG.
  // This allows us to replace this string with the actual file,
  // when we want to use or re-use the dag for different files.
  private val fileTag = "FILE_TAG"
  def dagString(dag: String, fileName: String): String = {
    dag.replace(fileTag, fileName)
  }
}
