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

/** Is a request or message to be sent to a server to
 *  query data and other extraneous processing of that data.
 *
 * @param processorName kind of processor (LambdaClearAll, LambdaReadAhead, LambdaInfo)
 * @param processorId
 * @param rowGroup
 * @param dag
 * @param accessTime The last time the file was touched.
 */
class ProcessorRequest(processorName: String,
                       processorId: String = "Unique Id",
                       rowGroup: Long = 0,
                       dag: String = "",
                       accessTime: Long = 1624464464409L) {

    private val logger = LoggerFactory.getLogger(getClass)

    def toXml : String = {
        val xmlof = XMLOutputFactory.newInstance()
        val strw = new StringWriter()
        val xmlw = xmlof.createXMLStreamWriter(strw)
        xmlw.writeStartDocument()
        xmlw.writeStartElement("Processor")

        xmlw.writeStartElement("Name")
        xmlw.writeCharacters(processorName)
        xmlw.writeEndElement() // Name

        xmlw.writeStartElement("ID")
        xmlw.writeCharacters(processorId)
        xmlw.writeEndElement() // Name

        xmlw.writeStartElement("Configuration")

        xmlw.writeStartElement("DAG")
        xmlw.writeCharacters(dag)
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
        // logger.info(xmlStr.replace("\n", "").replace("  ", ""))
        xmlStr.replace("\n", "").replace("  ", "")
    }
}
