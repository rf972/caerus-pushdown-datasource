// scalastyle:off
/*
 * Copyright 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Note that portions of this code came from spark-select code:
 *  https://github.com/minio/spark-select/blob/master/src/main/scala/io/minio/spark/select/FilterPushdown.scala
 *
 * Other portions of this code, most notably compileAggregates, and getColumnSchema,
 * came from this patch by Huaxin Gao:
 *   https://github.com/apache/spark/pull/29695
 */
// scalastyle:on
package com.github.datasource.common

import java.io.StringWriter
import java.sql.{Date, Timestamp}
import java.util
import java.util.{HashMap, Locale, StringTokenizer}
import javax.json.Json
import javax.json.JsonArrayBuilder
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import javax.json.JsonWriter

import scala.collection.mutable.ArrayBuilder

import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Max, Min, Sum}
import org.apache.spark.sql.types._


/** Provides pushdown capabilities aimed at
 *  generating information needed for pushdown
 *  from the inputs that Spark provides.
 */
class PushdownSQL(schema: StructType,
                  filters: Seq[Expression],
                  queryCols: Array[String]) {

  protected val logger = LoggerFactory.getLogger(getClass)
  /**
   * Use the given schema to look up the attribute's data type. Returns None if the attribute could
   * not be resolved.
   */
  private def getTypeForAttribute(attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }
  /**
   * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
   * condition will be added to the WHERE clause. If none of the filters can be pushed down then
   * an empty string will be returned.
   *
   * @param schema the schema of the table being queried
   * @param filters an array of filters, the conjunction of which is the filter condition for the
   *                scan.
   */
  def buildWhereClause(): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(f)).mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }
  /**
   * Attempt to convert the given filter into a Select expression. Returns None if the expression
   * could not be converted.
   */
  def buildFilterExpression(filter: Expression): Option[String] = {
    def buildComparison(expr1: Expression,
                        expr2: Expression,
                        comparisonOp: String): Option[String] = {
      val expr1_str = buildFilterExpression(expr1).getOrElse("")
      val expr2_str = buildFilterExpression(expr2).getOrElse("")
      Option(s"${expr1_str}" + s" $comparisonOp ${expr2_str}")
    }
    def buildLiteral(value: Any, dataType: DataType): Option[String] = {
      val sqlValue: String = dataType match {
        case StringType => s"""'${value.toString.replace("'", "\\'\\'")}'"""
        case DateType => s""""${value.asInstanceOf[Date]}""""
        case TimestampType => s""""${value.asInstanceOf[Timestamp]}""""
        case _ => value.toString
      }
      Option(sqlValue)
    }
    def buildOr(leftFilter: Option[String], rightFilter: Option[String]): Option[String] = {
      val left = leftFilter.getOrElse("")
      val right = rightFilter.getOrElse("")
      Option(s"""( $left OR $right )""")
    }
    def buildAnd(leftFilter: Option[String], rightFilter: Option[String]): Option[String] = {
      val left = leftFilter.getOrElse("")
      val right = rightFilter.getOrElse("")
      Option(s"""( $left AND $right )""")
    }
    def buildNot(filter: Option[String]): Option[String] = {
      val f = filter.getOrElse("")
      Option(s"""NOT ( $f )""")
    }
    def buildAttributeReference(attr: String): Option[String] = Option(attr)
    filter match {
      case Or(left, right) => buildOr(buildFilterExpression(left),
        buildFilterExpression(right))
      case And(left, right) => buildAnd(buildFilterExpression(left),
        buildFilterExpression(right))
      case Not(filter) => buildNot(buildFilterExpression(filter))
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      // When support is not there, do not push down IS NULL.
      // Allow the pushdown to continue without IS NULL,
      // to help evaluate pushdown.  For production consider to reject
      // the pushdown completely.
      case IsNull(attr) => if (true) {
        None // Option(s"${attr.name} IS NULL")
      } else {
        Option("TRUE")
      }
      // When support is not there, do not push down IS NULL.
      // Allow the pushdown to continue without IS NULL,
      // to help evaluate pushdown.  For production consider to reject
      // the pushdown completely.
      case IsNotNull(attr) => if (true) {
        None // Option(s"${attr.name} IS NOT NULL")
      } else {
        Option("TRUE")
      }
      /* case StringStartsWith(attr, value) =>
        Option(s"${attr} LIKE '${value}%'")
      case StringEndsWith(attr, value) =>
        Option(s"${attr} LIKE '%${value}'")
      case StringContains(attr, value) =>
        Option(s"${attr} LIKE '%${value}%'") */
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        buildAttributeReference(name)
      case Literal(value, dataType) =>
        buildLiteral(value, dataType)
      /* case Cast(expression, dataType, timeZoneId, _) =>
        buildFiltersJson(expression) */
      case other@_ => logger.info("unknown filter:" + other) ; None
    }
  }

  /** Returns a string to represent the input query.
   *
   * @return String representing the query to send to the endpoint.
   */
  def query: String = {
    var columnList = schema.fields.map(x => s"" + s"${x.name}").mkString(",")
    val whereClause = buildWhereClause()
    val objectClause = "TABLE_TAG"
    var retVal = ""
    val groupByClause = "" // getGroupByClause(aggregation)
    if (whereClause.length == 0) {
      retVal = s"SELECT $columnList FROM $objectClause $groupByClause"
    } else {
      retVal = s"SELECT $columnList FROM $objectClause $whereClause $groupByClause"
    }
    retVal
  }
  def columns: JsonArrayBuilder = {
    val arrayBuilder = Json.createArrayBuilder()
    queryCols.foreach(c => arrayBuilder.add(c))
    arrayBuilder
  }
  def jsonQuery: String = {
    val projectionNodeBuilder = Json.createObjectBuilder()
    projectionNodeBuilder.add("Name", "SQL")
    projectionNodeBuilder.add("Type", "_SQL")
    projectionNodeBuilder.add("Query", this.query)
    projectionNodeBuilder.add("Columns", this.columns)

    val stringWriter = new StringWriter()
    val writer = Json.createWriter(stringWriter)
    writer.writeObject(projectionNodeBuilder.build())
    writer.close()
    val jsonString = stringWriter.getBuffer().toString()
    // val indented = (new JSONObject(jsonString)).toString(4)
    jsonString
  }
}

object PushdownSQL {

  def apply(schema: StructType,
            filters: Seq[Expression],
            queryCols: Array[String]): PushdownSQL = {
    new PushdownSQL(schema, filters, queryCols)
  }
}