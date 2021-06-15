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
class ProcessorRequestParquet(accessTime: Long,
                              rowGroup: Long,
                              query: String,
                              blockSize: Long,
                              header: String) {

    private val logger = LoggerFactory.getLogger(getClass)

    def toXml : String = {
        val root = <Processor>
                     <Name>dikeSQL.parquet</Name>
                     <Version>0.1 </Version>
                     <Configuration>
                       <Query>{scala.xml.PCData(query)}</Query>
                       <RowGroupIndex>{rowGroup}</RowGroupIndex>
                       <LastAccessTime>{accessTime}</LastAccessTime>
                       <BlockSize>{blockSize}</BlockSize>
                       <HeaderInfo>{header}</HeaderInfo>
                     </Configuration>
                   </Processor>
        val writer = new StringWriter
        XML.write(writer, root, "UTF-8", true, null)
        writer.flush()
        logger.info(writer.toString.replace("\n", "").replace("  ", ""))
        writer.toString.replace("\n", "")
    }
}
