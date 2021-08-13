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

/** Is a request or message to be sent to a server to
 *  query data and other extraneous processing of that data.
 *
 * @param accessTime The last time the file was touched.
 * @param rowGroup the row group to read.
 * @param query the SQL operation to perform or empty string if none.
 * @param blockSize the length in bytes of the blocks we are reading.
 * @param header the type of header (NONE, IGNORE, SKIP).
 */
class ProcessorRequestLambda(accessTime: Long,
                             rowGroup: Long,
                             columnNames: Array[String],
                             fileName: String) {

    private val logger = LoggerFactory.getLogger(getClass)

    def toXml : String = {
        val xmlof = XMLOutputFactory.newInstance()
        val strw = new StringWriter()
        val xmlw = xmlof.createXMLStreamWriter(strw)
        xmlw.writeStartDocument()
        xmlw.writeStartElement("Processor")

        xmlw.writeStartElement("Name")
        xmlw.writeCharacters("Lambda")
        xmlw.writeEndElement() // Name

        xmlw.writeStartElement("Configuration")

        xmlw.writeStartElement("DAG")
        val dagBuilder = Json.createObjectBuilder()
        dagBuilder.add("Name", "DAG Projection")

        val inputNodeBuilder = Json.createObjectBuilder()
        inputNodeBuilder.add("Name", "InputNode")
        inputNodeBuilder.add("Type", "_INPUT")
        inputNodeBuilder.add("File", fileName)

        val projectionNodeBuilder = Json.createObjectBuilder()
        projectionNodeBuilder.add("Name", "TpchQ1")
        projectionNodeBuilder.add("Type", "_PROJECTION")
        val projectionArrayBuilder = Json.createArrayBuilder()

        for (col <- columnNames) {
          projectionArrayBuilder.add(col)
        }

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder)

        val optputNodeBuilder = Json.createObjectBuilder()
        optputNodeBuilder.add("Name", "OutputNode")
        optputNodeBuilder.add("Type", "_OUTPUT")

        val nodeArrayBuilder = Json.createArrayBuilder()
        nodeArrayBuilder.add(inputNodeBuilder.build())
        nodeArrayBuilder.add(projectionNodeBuilder.build())
        nodeArrayBuilder.add(optputNodeBuilder.build())

        dagBuilder.add("NodeArray", nodeArrayBuilder)

        // For now we will assume simple pipe with ordered connections
        val dag = dagBuilder.build()

        val stringWriter = new StringWriter()
        val writer = Json.createWriter(stringWriter)
        writer.writeObject(dag)
        writer.close()

        xmlw.writeCharacters(stringWriter.getBuffer().toString())
        xmlw.writeEndElement() // DAG

        xmlw.writeStartElement("RowGroupIndex")
        xmlw.writeCharacters(rowGroup.toString)
        xmlw.writeEndElement() // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime")
        xmlw.writeCharacters(accessTime.toString)
        xmlw.writeEndElement() // LastAccessTime

        xmlw.writeEndElement() // Configuration
        xmlw.writeEndElement() // Processor
        xmlw.writeEndDocument()
        xmlw.close()

        val xmlStr = strw.toString()
        logger.info(xmlStr.replace("\n", "").replace("  ", ""))
        xmlStr.replace("\n", "")
    }
}
