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
package com.github.datasource.parse

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.text.{NumberFormat, SimpleDateFormat}
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.Try

import com.github.datasource.common.TypeCast
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.MessageColumnIO
import org.apache.parquet.io.RecordReader
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.Type
import org.slf4j.LoggerFactory

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/** Iterator object that allows for parsing
 *  Parquet Columns into InternalRow structures.
 *
 * @param rowReader the bufferedReader to fetch data
 * @param schema the format of this stream of data
 */
class ParquetRowIterator(reader: ParquetFileReader,
                         dataTypes: Array[DataType])
  extends Iterator[InternalRow] {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Returns an InternalRow parsed from the input current Row.
   *
   * @param line the String of line to parse
   * @return the InternalRow of this line..
   */
  private def getInternalRow(rowData: Group): InternalRow = {
    val fieldCount = rowData.getType().getFieldCount()
    var row = new Array[Any](dataTypes.length)
    for (field <- 0 to fieldCount - 1) {
      val data = rowData.getValueToString(field, 0)
      row(field) = TypeCast.castTo(data, dataTypes(field))
    }
    if (fieldCount >= dataTypes.length) {
      // logger.info("row: " + row.mkString(", "))
      new GenericInternalRow(row)
    } else {
      /* We did not read a full row.  Return an empty row since the row is not valid.
       */
      InternalRow.empty
    }
  }
  private var rowIndex: Long = 0
  private var totalRows: Long = 0
  private var recordReader: RecordReader[Group] = null

  def getNextReader(): Unit = {
    logger.info(s"page rows ${totalRows}")
    val pages: PageReadStore = reader.readNextRowGroup()
    val schema = reader.getFileMetaData().getSchema()
    val columnIO: MessageColumnIO = new ColumnIOFactory().getColumnIO(schema)
    rowIndex = 0
    if (pages != null) {
      totalRows = pages.getRowCount()
      logger.info(s"page rows ${totalRows}")
      recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
    } else {
      totalRows = 0
      recordReader = null
    }
  }
  def getNumRows(): Long = totalRows
  /** Returns the next row or if none, InternalRow.empty.
   *
   * @return InternalRow for the next row.
   */
  private var nextRow: InternalRow = {
    getNextRow()
  }
  /** Returns row following the current one,
   *  (if availble), by parsing the next line.
   *
   * @return the next InternalRow object or InternalRow.empty if none.
   */
  private var rows: Long = 0
  private var lastRow: InternalRow = InternalRow.empty
  private def getNextRow(): InternalRow = {
    if (rowIndex >= totalRows) {
      getNextReader
    }
    if (rowIndex < totalRows && recordReader != null) {
      val group: Group = recordReader.read()
      rowIndex += 1
      getInternalRow(group)
    } else {
      InternalRow.empty
    }
  }
  /** Returns true if there are remaining rows.
   *
   * @return true if rows remaining, false otherwise.
   */
  override def hasNext: Boolean = {
    nextRow.numFields > 0
  }
  /** Returns the following InternalRow
   *
   * @return the next InternalRow or InternalRow.empty if none.
   */
  override def next: InternalRow = {
    val row = nextRow
    nextRow = getNextRow()
    row
  }
  def close(): Unit = {}
}

object ParquetRowIterator {
  private val logger = LoggerFactory.getLogger(getClass)

  def apply(reader: ParquetFileReader,
            dataTypes: Array[DataType]): ParquetRowIterator = {
    new ParquetRowIterator(reader, dataTypes)
  }
}
