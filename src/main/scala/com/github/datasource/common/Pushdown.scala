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

import java.sql.{Date, Timestamp}
import java.util
import java.util.{HashMap, Locale, StringTokenizer}

import scala.collection.mutable.ArrayBuilder

import org.slf4j.LoggerFactory

import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._


/** Provides pushdown capabilities aimed at
 *  generating information needed for pushdown
 *  from the inputs that Spark provides.
 */
class Pushdown(val schema: StructType, val prunedSchema: StructType,
               val filters: Seq[Filter],
               val aggregation: Option[Aggregation],
               val options: util.Map[String, String]) extends Serializable {

  protected val logger = LoggerFactory.getLogger(getClass)

  protected var supportsIsNull = !options.containsKey("DisableSupportsIsNull")

  def isPushdownNeeded: Boolean = {
    /* Determines if we should send the pushdown to ndp.
     * If any of the pushdowns are in use (project, filter, aggregate),
     * then we will consider that pushdown is needed.
     */
    ((prunedSchema.length != schema.length) ||
     (filters.length > 0) ||
     ( (aggregation != None) &&
       ((aggregation.get.aggregateExpressions.length > 0) ||
        (aggregation.get.groupByColumns.length > 0))))
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
  def buildFilterExpression(filter: Filter): Option[String] = {
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[String] = {
      getTypeForAttribute(attr).map { dataType =>
        val sqlEscapedValue: String = dataType match {
          case StringType => s"""'${value.toString.replace("'", "\\'\\'")}'"""
          case DateType => s""""${value.asInstanceOf[Date]}""""
          case TimestampType => s""""${value.asInstanceOf[Timestamp]}""""
          case _ => value.toString
        }
        s"${getColString(attr)}" + s" $comparisonOp $sqlEscapedValue"
      }
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
      case IsNull(attr) => if (supportsIsNull) {
          Option(s"${getColString(attr)} IS NULL")
        } else {
          Option("TRUE")
        }
      // When support is not there, do not push down IS NULL.
      // Allow the pushdown to continue without IS NULL,
      // to help evaluate pushdown.  For production consider to reject
      // the pushdown completely.
      case IsNotNull(attr) => if (supportsIsNull) {
          Option(s"${getColString(attr)} IS NOT NULL")
        } else {
          Option("TRUE")
        }
      case StringStartsWith(attr, value) =>
        Option(s"${getColString(attr)} LIKE '${value}%'")
      case StringEndsWith(attr, value) =>
        Option(s"${getColString(attr)} LIKE '%${value}'")
      case StringContains(attr, value) =>
        Option(s"${getColString(attr)} LIKE '%${value}%'")
      case other@_ => logger.info("unknown filter:" + other) ; None
    }
  }

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
  def quoteIdentifier(colName: String): String = {
    s"${getColString(colName)}"
  }
  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  def getColumnSchema(): (String, StructType) = {

    val (compiledAgg, aggDataType) = compileAggregates(aggregation)
    val sb = new StringBuilder()
    val columnNames = prunedSchema.map(_.name).toArray
    var updatedSchema: StructType = new StructType()
    if (compiledAgg.length == 0) {
      val cols = prunedSchema.fields.map(x => {
            getColString(x.name)
        }).toArray
      updatedSchema = prunedSchema
      cols.foreach(x => sb.append(",").append(x))
    } else {
      updatedSchema = getAggregateColumnsList(sb, compiledAgg, aggDataType)
    }
    (if (sb.length == 0) "" else sb.substring(1),
     if (sb.length == 0) prunedSchema else updatedSchema)
  }
  private def containsArithmeticOp(col: String): Boolean =
    col.contains("+") || col.contains("-") ||
    col.contains("*") || col.contains("/") ||
    col.contains("(") || col.contains(")")

  private def getStructFieldForCol(col: String): StructField =
    schema.fields(schema.fieldNames.toList.indexOf(col))
  /** Returns an array of aggregates translated to strings.
   *
   * @param aggregates the array of aggregates to translate
   * @return array of strings
   */
  def compileAggregates(agg: Option[Aggregation]) : (Array[String], Array[DataType]) = {
    val aggregates: Seq[AggregateFunc] = {
      if (agg == None) {
        Seq.empty[AggregateFunc]
      } else {
        agg.get.aggregateExpressions
      }
    }
    def quote(colName: String): String = quoteIdentifier(colName)
    val aggBuilder = ArrayBuilder.make[String]
    val dataTypeBuilder = ArrayBuilder.make[DataType]
    aggregates.map {
      case min: Min =>
        dataTypeBuilder += getStructFieldForCol(min.column.fieldNames.head).dataType
        if (!containsArithmeticOp(min.column.fieldNames.head)) {
          aggBuilder += s"MIN(${quote(min.column.fieldNames.head)})"
        } else {
          aggBuilder += s"MIN(${quoteEachCols(min.column.fieldNames.head)})"
        }
      case max: Max =>
        val colName = max.column.fieldNames.head
        dataTypeBuilder += getStructFieldForCol(colName).dataType
        if (!containsArithmeticOp(colName)) {
          aggBuilder += s"MAX(${quote(colName)})"
        } else {
          aggBuilder += s"MAX(${quoteEachCols(colName)})"
        }
      case sum: Sum =>
        val colName = sum.column.fieldNames.head
        val distinct = if (sum.isDistinct) "DISTINCT " else ""
        dataTypeBuilder += getStructFieldForCol(colName).dataType
        if (!containsArithmeticOp(colName)) {
          aggBuilder += s"SUM(${distinct} ${quote(colName)})"
        } else {
          aggBuilder += s"SUM(${distinct}${quoteEachCols(colName)})"
        }
      case count: Count =>
        val colName = count.column.fieldNames.head
        val distinct = if (count.isDistinct) "DISTINCT " else ""
        dataTypeBuilder += getStructFieldForCol(colName).dataType
        if (!containsArithmeticOp(colName)) {
          aggBuilder += s"COUNT(${distinct}${quote(colName)})"
        } else {
          aggBuilder += s"COUNT(${distinct}${quoteEachCols(colName)})"
        }
      case _: CountStar =>
        dataTypeBuilder += LongType
        aggBuilder += s"count(*)"
      case _ =>
    }
    (aggBuilder.result, dataTypeBuilder.result)
  }

  private def quoteEachCols (column: String): String = {
    def quote(colName: String): String = quoteIdentifier(colName)
    val colsBuilder = ArrayBuilder.make[String]
    val st = new StringTokenizer(column, "+-*/()", true)
    colsBuilder += quote(st.nextToken().trim)
    while (st.hasMoreTokens) {
      colsBuilder += quote(st.nextToken().trim)
    }
    colsBuilder.result.mkString(" ")
  }
  private def getAggregateColumnsList(sb: StringBuilder,
                                      compiledAgg: Array[String],
                                      aggDataType: Array[DataType]) = {
    val columnNames = prunedSchema.map(_.name).toArray
    val quotedColumns: Array[String] =
      columnNames.map(colName => quoteIdentifier(colName.toLowerCase(Locale.ROOT)))
    val colDataTypeMap: Map[String, StructField] = quotedColumns.zip(prunedSchema.fields).toMap
    val newColsBuilder = ArrayBuilder.make[String]
    var updatedSchema: StructType = new StructType()

    if (aggregation != None) {
      for (groupBy <- aggregation.get.groupByColumns) {
        val quotedGroupBy = quoteIdentifier(groupBy.fieldNames.head)
        newColsBuilder += quotedGroupBy
        updatedSchema = updatedSchema.add(colDataTypeMap.get(quotedGroupBy).get)
      }
    }
    for ((col, dataType) <- compiledAgg.zip(aggDataType)) {
      newColsBuilder += col
      updatedSchema = updatedSchema.add(col, dataType)
    }
    sb.append(", ").append(newColsBuilder.result.mkString(", "))
    updatedSchema
  }

  private def contains(s1: String, s2: String, checkParathesis: Boolean): Boolean = {
    if (false /* SQLConf.get.caseSensitiveAnalysis */) {
      if (checkParathesis) s1.contains("(" + s2) else s1.contains(s2)
    } else {
      if (checkParathesis) {
        s1.toLowerCase(Locale.ROOT).contains("(" + s2.toLowerCase(Locale.ROOT))
      } else {
        s1.toLowerCase(Locale.ROOT).contains(s2.toLowerCase(Locale.ROOT))
      }
    }
  }

  def quoteIdentifierGroupBy(colName: String): String = {
    s"${getColString(colName)}"
  }
  private def getGroupByClause(aggregation: Option[Aggregation]): String = {
    if ((aggregation != None) &&
        (aggregation.get.groupByColumns.length > 0)) {
      val quotedColumns =
          aggregation.get.groupByColumns.map(c => s"${getColString(c.fieldNames.head)}")
      s"GROUP BY ${quotedColumns.mkString(", ")}"
    } else {
      ""
    }
  }

  /** returns the representation of the column name according to the
   *  current option set.
   *  @param attr - Attribute name
   *  @param disableCast - true to disable any casting.
   *  @return String - representation of the column name.
   */
  def getColString(attr: String, disableCast: Boolean = false): String = {
    var allowCast = !disableCast
    val colString =
      if (options.containsKey("useColumnNames")) {
        s"${attr}"
      } else {
        val index = getSchemaIndex(attr)
        // If the attribute is not recognized, it might
        // be an operator.  Just allow it to be inserted
        // as is without any cast.
        if (index == -1) {
          allowCast = false
          attr
        } else {
          s"_${index}"
        }
      }
    if (options.containsKey("DisableCasts") || !allowCast) {
      colString
    } else {
      getColCastString(colString, attr)
    }
  }

  /** returns the index of a field matching an input name in a schema.
   *
   * @param name the name of the field to search for in schema
   *
   * @return Integer - The index of this field in the input schema.
   */
  def getSchemaIndex(name: String): Integer = {
    for (i <- 0 to schema.fields.size - 1) {
      if (schema.fields(i).name == name) {
        return i + 1
      }
    }
    -1
  }
  private val disabledCasts = {
    val userDisabledCasts: String = {
      if (options.containsKey("DisabledCasts")) {
        options.get("DisabledCasts")
      } else {
        ""
      }}
    s"TIMESTAMP,STRING,${userDisabledCasts}".split(",")
  }
  logger.trace(s"disabledCasts ${disabledCasts.mkString(",")}")

  /** returns the representation of the column, cast to a
   *  specific value.
   *  @param colString - Representation of column chosen by caller
   *  @param colName - Name of column in schema
   *  @return String - representation of the column name.
   */
  def getColCastString(colString: String, colName: String): String = {
    val attrType = getTypeForAttribute(colName).getOrElse(StringType)
    /* We match the type to the types specified by AWS S3 Select.
     */
    val mappedType = attrType match {
      case _: BooleanType => "BOOL"
      case _: IntegerType => "INTEGER"
      case _: LongType => "INTEGER"
      case _: FloatType => "FLOAT"
      case _: DecimalType => "NUMERIC"
      case _: DoubleType => "NUMERIC"
      case _: StringType => "STRING"
      case _: DateType => "TIMESTAMP"
      case _ => s"UNKNOWN_${attrType}"
    }
    if (disabledCasts.contains(mappedType)) {
      s"${colString}"
    } else {
      s"CAST(${colString} as ${mappedType})"
    }
  }

  /** Returns a string to represent the input query.
   *
   * @return String representing the query to send to the endpoint.
   */
  def queryFromSchema(partition: PushdownPartition): String = {
    var columnList = ""
    if (readColumns.length > 0) {
      columnList = readColumns
    } else {
      columnList = readSchema.fields.map(x => s"" +
                                         s"${getColString(x.name)}").mkString(",")
      if (columnList.length == 0) {
        columnList = "*"
      }
    }
    val whereClause = buildWhereClause()
    val objectClause = partition.getObjectClause(partition)
    var retVal = ""
    val groupByClause = getGroupByClause(aggregation)
    if (whereClause.length == 0) {
      retVal = s"SELECT $columnList FROM $objectClause $groupByClause"
    } else {
      retVal = s"SELECT $columnList FROM $objectClause $whereClause $groupByClause"
    }
    retVal
  }
  /** Determines if we can push down this aggregate operation.
   *  @return Boolean true if valid, false otherwise
   */
  def aggregatePushdownValid(): Boolean = {
    val (compiledAgg, aggDataType) =
      compileAggregates(aggregation)
    if (compiledAgg.isEmpty == false) {
      var valid = true
      /* Disable aggregate pushdown if it contains distinct,
       * and if the DisableDistinct option is set.
       */
      if (options.containsKey("DisableDistinct")) {
        for (agg <- compiledAgg) {
          if (agg.contains("DISTINCT")) {
            valid = false
          }
        }
      }
      ((!options.containsKey("DisableGroupbyPush") ||
        ((aggregation != None) &&
         (aggregation.get.groupByColumns.length == 0))) &&
       valid)
    } else {
      true
    }
  }
  val (readColumns: String,
       readSchema: StructType) = {
    var (columns, updatedSchema) =
      getColumnSchema()
    (columns,
     if (updatedSchema.names.isEmpty) schema else updatedSchema)
  }
  /** Returns a Query that is fully readable and does
   *  not contain any casts or column numbers..
   *
   *  @param partition the partition to render this query.
   *  @return String a string of the query.
   */
  def getReadableQuery(partition: PushdownPartition): String = {
    val optionsCopy = new HashMap[String, String](options)
    // Set options to render query readable.
    // Set option to represent columns with names instead of numbers.
    // Set option to not use casts
    optionsCopy.put("useColumnNames", "")
    optionsCopy.put("DisableCasts", "")
    val pd = new Pushdown(schema, prunedSchema,
                          filters, aggregation,
                          optionsCopy)
    pd.queryFromSchema(partition)
  }
}

/**
 * Helper methods for pushing filters into Select queries.
 */
object Pushdown {

  protected val logger = LoggerFactory.getLogger(getClass)

  /** Returns a string to represent the schema of the table.
   *
   * @param schema the StructType representing the definition of columns.
   * @return String representing the table's columns.
   */
  def schemaString(schema: StructType): String = {

    schema.fields.map(x => {
      val dataTypeString = {
        x.dataType match {
        case IntegerType => "INTEGER"
        case LongType => "LONG"
        case DoubleType => "NUMERIC"
        case _ => "STRING"
        }
      }
      s"${x.name} ${dataTypeString}"
    }).mkString(", ")
  }
}
