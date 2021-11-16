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

package com.github.datasource.common

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

import PushdownJsonStatus._
import org.json._
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max, Min, Sum}
import org.apache.spark.sql.execution.datasources.PushableColumnAndNestedColumn
import org.apache.spark.sql.execution.datasources.PushableColumnWithoutNestedColumn
import org.apache.spark.sql.types._

object PushdownJson {

  private val filterMaxDepth = 2

  protected val logger = LoggerFactory.getLogger(getClass)

  def buildFiltersJson(expr: Expression): JsonObject = {
    def buildComparison(left: Expression, right: Expression, comparisonOp: String): JsonObject = {
      val compNodeBuilder = Json.createObjectBuilder()
      compNodeBuilder.add("Expression", comparisonOp)
      compNodeBuilder.add("Left", buildFiltersJson(left))
      compNodeBuilder.add("Right", buildFiltersJson(right))
      compNodeBuilder.build()
    }
    def buildOr(leftFilter: JsonObject, rightFilter: JsonObject): JsonObject = {
      val orNodeBuilder = Json.createObjectBuilder()
      orNodeBuilder.add("Expression", "Or")
      orNodeBuilder.add("Left", leftFilter)
      orNodeBuilder.add("Right", rightFilter)
      orNodeBuilder.build()
    }
    def buildAnd(leftFilter: JsonObject, rightFilter: JsonObject): JsonObject = {
      val orNodeBuilder = Json.createObjectBuilder()
      orNodeBuilder.add("Expression", "And")
      orNodeBuilder.add("Left", leftFilter)
      orNodeBuilder.add("Right", rightFilter)
      orNodeBuilder.build()
    }
    def buildNot(filter: JsonObject): JsonObject = {
      val NotNodeBuilder = Json.createObjectBuilder()
      NotNodeBuilder.add("Expression", "Not")
      NotNodeBuilder.add("Arg", filter)
      NotNodeBuilder.build()
    }
    def buildIsNull(value: JsonObject): JsonObject = {
      val isNullNodeBuilder = Json.createObjectBuilder()
      isNullNodeBuilder.add("Expression", "IsNull")
      isNullNodeBuilder.add("Arg", value)
      isNullNodeBuilder.build()
    }
    def buildIsNotNull(value: JsonObject): JsonObject = {
      val isNullNodeBuilder = Json.createObjectBuilder()
      isNullNodeBuilder.add("Expression", "IsNotNull")
      isNullNodeBuilder.add("Arg", value)
      isNullNodeBuilder.build()
    }
    def buildUnknown(filter: Expression): JsonObject = {
        val ukNodeBuilder = Json.createObjectBuilder()
        ukNodeBuilder.add("Expression", "Unknown")
        ukNodeBuilder.add("Arg", filter.toString)
        ukNodeBuilder.build()
    }
    def buildGeneric(name: String, value: String): JsonObject = {
        val colRefBuilder = Json.createObjectBuilder()
        colRefBuilder.add(name, value)
        colRefBuilder.build()
    }
    def buildExpression(operator: String,
                        left: JsonObject, right: JsonObject): JsonObject = {
      val orNodeBuilder = Json.createObjectBuilder()
      orNodeBuilder.add("Expression", operator)
      orNodeBuilder.add("Left", left)
      orNodeBuilder.add("Right", right)
      orNodeBuilder.build()
    }
    expr match {
      case Or(left, right) => buildOr(buildFiltersJson(left),
                                      buildFiltersJson(right))
      case And(left, right) => buildAnd(buildFiltersJson(left),
                                        buildFiltersJson(right))
      case Not(filter) => buildNot(buildFiltersJson(filter))
      case EqualTo(attr, value) => buildComparison(attr, value, "EqualTo")
      case LessThan(attr, value) => buildComparison(attr, value, "LessThan")
      case GreaterThan(attr, value) => buildComparison(attr, value, "GreaterThan")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "LessThanOrEqual")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, "GreaterThanOrEqual")
      case IsNull(attr) => buildIsNull(buildFiltersJson(attr))
      case IsNotNull(attr) => buildIsNotNull(buildFiltersJson(attr))
      case StartsWith(left, right) => buildExpression("StartsWith",
                                                      buildFiltersJson(left),
                                                      buildFiltersJson(right))
      case EndsWith(left, right) => buildExpression("EndsWith",
                                                    buildFiltersJson(left),
                                                    buildFiltersJson(right))
      case Contains(left, right) => buildExpression("Contains",
                                                    buildFiltersJson(left),
                                                    buildFiltersJson(right))
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        buildGeneric("ColumnReference", name)
      case Literal(value, dataType) =>
        buildGeneric("Literal", value.toString)
      case Cast(expression, dataType, timeZoneId) =>
        buildFiltersJson(expression)
      case other@_ => logger.warn("unknown filter:" + other)
         buildUnknown(other)
    }
  }
  def getFiltersJson(filters: Seq[Expression], test: String): String = {
    val filterNodeBuilder = Json.createObjectBuilder()
    filterNodeBuilder.add("Name", test)
    filterNodeBuilder.add("Type", "_FILTER")
    val filterArrayBuilder = Json.createArrayBuilder()
    for (f <- filters) {
      if (validateFilterExpression(f)) {
        val j = buildFiltersJson(f)
        filterArrayBuilder.add(j)
      }
    }
    filterNodeBuilder.add("FilterArray", filterArrayBuilder)

    val stringWriter = new StringWriter()
    val writer = Json.createWriter(stringWriter)
    writer.writeObject(filterNodeBuilder.build())
    writer.close()
    val jsonString = stringWriter.getBuffer().toString()
    // val indented = (new JSONObject(jsonString)).toString(4)
    jsonString
  }
  def getFiltersJsonMaxDesired(filters: Seq[Expression], test: String): String = {
    val filterNodeBuilder = Json.createObjectBuilder()
    filterNodeBuilder.add("Name", test)
    filterNodeBuilder.add("Type", "_FILTER")
    val filterArrayBuilder = Json.createArrayBuilder()
    for (f <- filters) {
      if (validateMaxDesiredFilter(f)) {
        val j = buildFiltersJson(f)
        filterArrayBuilder.add(j)
      }
    }
    filterNodeBuilder.add("FilterArray", filterArrayBuilder)

    val stringWriter = new StringWriter()
    val writer = Json.createWriter(stringWriter)
    writer.writeObject(filterNodeBuilder.build())
    writer.close()
    val jsonString = stringWriter.getBuffer().toString()
    val indented = (new JSONObject(jsonString)).toString(4)
    indented
  }
  def validateFilterExpression(expr: Expression, depth: Int = 0): Boolean = {
    if (depth > filterMaxDepth) {
      /* Reached depth unsupported by NDP server. */
      return false
    }
    /* Traverse the tree and validate the nodes are supported. */
    expr match {
      case Or(left, right) => validateFilterExpression(left, depth + 1) &&
                              validateFilterExpression(right, depth + 1)
      /* case And(left, right) => validateFilterExpression(left, depth + 1) &&
                              validateFilterExpression(right, depth + 1)
      case Not(filter) => validateFilterExpression(filter, depth + 1) */
      case EqualTo(left, right) => validateFilterExpression(left, depth + 1) &&
                                   validateFilterExpression(right, depth + 1)
      case LessThan(left, right) => validateFilterExpression(left, depth + 1) &&
                                     validateFilterExpression(right, depth + 1)
      case GreaterThan(left, right) => validateFilterExpression(left, depth + 1) &&
                                       validateFilterExpression(right, depth + 1)
      case LessThanOrEqual(left, right) => validateFilterExpression(left, depth + 1) &&
                                           validateFilterExpression(right, depth + 1)
      case GreaterThanOrEqual(left, right) => validateFilterExpression(left, depth + 1) &&
                                              validateFilterExpression(right, depth + 1)
      case IsNull(attr) => validateFilterExpression(attr, depth + 1)
      case IsNotNull(attr) => validateFilterExpression(attr, depth + 1)
      /* case StartsWith(left, right) => validateFilterExpression(left, depth + 1) &&
                                    validateFilterExpression(right, depth + 1)
      case EndsWith(left, right) => validateFilterExpression(left, depth + 1) &&
                                    validateFilterExpression(right, depth + 1)
      case Contains(left, right) => validateFilterExpression(left, depth + 1) &&
                                    validateFilterExpression(right, depth + 1) */
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        true
      case Literal(value, dataType) =>
        true
      case Cast(expression, dataType, timeZoneId) =>
        true
      case other@_ => logger.warn("unknown filter:" + other)
        /* Reached an unknown node, return validation failed. */
        false
    }
  }
  def validateMaxDesiredFilter(expr: Expression, depth: Int = 0): Boolean = {
    /* Traverse the tree and validate the nodes are supported. */
    expr match {
      case Or(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                              validateMaxDesiredFilter(right, depth + 1)
      case And(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                              validateMaxDesiredFilter(right, depth + 1)
      case Not(filter) => validateMaxDesiredFilter(filter, depth + 1)
      case EqualTo(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                                   validateMaxDesiredFilter(right, depth + 1)
      case LessThan(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                                     validateMaxDesiredFilter(right, depth + 1)
      case GreaterThan(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                                       validateMaxDesiredFilter(right, depth + 1)
      case LessThanOrEqual(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                                           validateMaxDesiredFilter(right, depth + 1)
      case GreaterThanOrEqual(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                                              validateMaxDesiredFilter(right, depth + 1)
      case IsNull(attr) => validateMaxDesiredFilter(attr, depth + 1)
      case IsNotNull(attr) => validateMaxDesiredFilter(attr, depth + 1)
      case StartsWith(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                                    validateMaxDesiredFilter(right, depth + 1)
      case EndsWith(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                                    validateMaxDesiredFilter(right, depth + 1)
      case Contains(left, right) => validateMaxDesiredFilter(left, depth + 1) &&
                                    validateMaxDesiredFilter(right, depth + 1)
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        true
      case Literal(value, dataType) =>
        true
      case Cast(expression, dataType, timeZoneId) =>
        true
      case other@_ => logger.warn("unknown filter:" + other)
        /* Reached an unknown node, return validation failed. */
        false
    }
  }
    def validateFilters(filters: Seq[Expression]): PushdownJsonStatus = {
      var status: Boolean = true
      var invalidCount = 0
      var validCount = 0
      for (f <- filters) {
        if (validateFilterExpression(f)) {
          validCount += 1
        } else {
          invalidCount += 1
        }
      }
      if (invalidCount == 0 && validCount > 0) {
        PushdownJsonStatus.FullyValid
      } else if (invalidCount > 0 && validCount > 0) {
        PushdownJsonStatus.PartiallyValid
      } else {
        PushdownJsonStatus.Invalid
      }
    }
    def getProjectJson(columnNames: Seq[String],
                       test: String): String = {
      val projectionNodeBuilder = Json.createObjectBuilder()
      if (columnNames.length > 0) {
        projectionNodeBuilder.add("Name", test)
        projectionNodeBuilder.add("Type", "_PROJECTION")
        val projectionArrayBuilder = Json.createArrayBuilder()

        for (col <- columnNames) {
          projectionArrayBuilder.add(col)
        }
        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder)
      }
      val stringWriter = new StringWriter()
      val writer = Json.createWriter(stringWriter)
      writer.writeObject(projectionNodeBuilder.build())
      writer.close()
      val jsonString = stringWriter.getBuffer().toString()
      // val indented = (new JSONObject(jsonString)).toString(4)
      jsonString
    }
    def getAggregateName(aggregateExpression: AggregateExpression,
                         topAggregate: Boolean = false): Option[String] = {
      aggregateExpression.aggregateFunction match {
        case Min(PushableColumnWithoutNestedColumn(name)) =>
          Some(s"min($name)")
        case Max(PushableColumnWithoutNestedColumn(name)) =>
          Some(s"max($name)")
        case sum @ Sum(PushableColumnWithoutNestedColumn(name)) =>
          if (topAggregate) Some(s"sum(sum($name))")
          else Some(s"sum($name)")
        case _ => None
      }
    }
    def getAggregateJson(groupingExpressions: Seq[Expression],
                         aggregateExpressions: Seq[AggregateExpression],
                         test: String,
                         topAggregate: Boolean = false): String = {
      val aggNodeBuilder = Json.createObjectBuilder()
      aggNodeBuilder.add("Name", test)
      aggNodeBuilder.add("Type", "_AGGREGATE")

      if (topAggregate) aggNodeBuilder.add("Barrier", "1")

      val groupingArrayBuilder = Json.createArrayBuilder()
      for (f <- groupingExpressions) {
        val j = buildGroupingExpressionJson(f)
        if (j.isDefined) {
          groupingArrayBuilder.add(j.get)
        }
      }
      aggNodeBuilder.add("GroupingArray", groupingArrayBuilder)

      val aggregateArrayBuilder = Json.createArrayBuilder()
      for (f <- aggregateExpressions) {
          val j = buildAggregateExpressionJson(f, topAggregate)
          if (j.isDefined) {
            aggregateArrayBuilder.add(j.get)
          }
      }
      aggNodeBuilder.add("AggregateArray", aggregateArrayBuilder)
      val stringWriter = new StringWriter()
      val writer = Json.createWriter(stringWriter)
      writer.writeObject(aggNodeBuilder.build())
      writer.close()
      val jsonString = stringWriter.getBuffer().toString()
      // val indented = (new JSONObject(jsonString)).toString(4)
      jsonString
  }
  def buildGroupingExpressionJson(e: Expression): Option[JsonObject] = {
    e match {
        case PushableColumnWithoutNestedColumn(name) =>
          Some(buildGeneric("ColumnReference", name))
        case _ => None
    }
  }

  def getAggregateSchema(aggregates: Seq[AggregateExpression],
                         groupingExpressions: Seq[Expression]): StructType = {
    var schema = new StructType()
    for (e <- groupingExpressions) {
      e match {
        case attrib @ AttributeReference(name, dataType, nullable, meta) =>
          schema = schema.add(StructField(name, dataType))
      }
    }
    for (a <- aggregates) {
      a.aggregateFunction match {
        case min @ Min(PushableColumnWithoutNestedColumn(name)) =>
          schema = schema.add(StructField(s"min(${name})", min.dataType))
        case max @ Max(PushableColumnWithoutNestedColumn(name)) =>
          schema = schema.add(StructField(s"max(${name})", max.dataType))
        case count: aggregate.Count if count.children.length == 1 =>
          count.children.head match {
            // SELECT COUNT(*) FROM table is translated to SELECT 1 FROM table
            case Literal(_, _) =>
              schema = schema.add(StructField("count(*)", LongType))
            case PushableColumnWithoutNestedColumn(name) =>
              schema = schema.add(StructField(s"count(${name})", LongType))
            case _ => None
          }
        case sum @ Sum(PushableColumnWithoutNestedColumn(name)) =>
          schema = schema.add(StructField(s"sum(${name})", sum.dataType))
        case sum @ Sum(child: Expression) =>
          schema = schema.add(StructField(s"sum(${getAggregateString(child)})", sum.dataType))
        case _ => None
      }
    }
    schema
  }
  def getAggregateString(e: Expression): String = {
    e match {
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        name
      case Literal(value, dataType) =>
        value.toString
      case mult @ Multiply(left, right, failOnError) =>
        s"(${getAggregateString(left)} * ${getAggregateString(right)})"
      case div @ Divide(left, right, failOnError) =>
        s"(${getAggregateString(left)} / ${getAggregateString(right)})"
      case add @ Add(left, right, failOnError) =>
        s"(${getAggregateString(left)} + ${getAggregateString(right)})"
      case subtract @ Subtract(left, right, failOnError) =>
        s"(${getAggregateString(left)} - ${getAggregateString(right)})"
    }
  }
  def buildAggregateExpressionJson(aggregate: AggregateExpression,
                                   topAggregate: Boolean = false): Option[JsonObject] = {
    def buildAggregate(name: String, value: JsonObject): JsonObject = {
      val aggNodeBuilder = Json.createObjectBuilder()
      aggNodeBuilder.add("Aggregate", name)
      aggNodeBuilder.add("Expression", value)
      aggNodeBuilder.build()
    }
    def buildAggComp(left: Expression, right: Expression, comparisonOp: String): JsonObject = {
      val compNodeBuilder = Json.createObjectBuilder()
      compNodeBuilder.add("Expression", comparisonOp)
      compNodeBuilder.add("Left", buildAggExpr(left))
      compNodeBuilder.add("Right", buildAggExpr(right))
      compNodeBuilder.build()
    }
    def buildAggExpr(e: Expression) : JsonObject = {
      e match {
        case attrib @ AttributeReference(name, dataType, nullable, meta) =>
          buildGeneric("ColumnReference", name)
        case Literal(value, dataType) =>
          buildGeneric("Literal", value.toString)
        case mult @ Multiply(left, right, failOnError) =>
          buildAggComp(left, right, "multiply")
        case div @ Divide(left, right, failOnError) =>
          buildAggComp(left, right, "divide")
        case add @ Add(left, right, failOnError) =>
          buildAggComp(left, right, "add")
        case subtract @ Subtract(left, right, failOnError) =>
          buildAggComp(left, right, "subtract")
      }
    }
    def buildCount(name: String, value: JsonObject,
                   isDistinct: Boolean = false): JsonObject = {
      val aggNodeBuilder = Json.createObjectBuilder()
      aggNodeBuilder.add("Aggregate", name)
      aggNodeBuilder.add("Expression", value)
      aggNodeBuilder.add("Distinct", if (isDistinct) { "yes" } else { "no" } )
      aggNodeBuilder.build()
    }
    if (aggregate.filter.isEmpty) {
      aggregate.aggregateFunction match {
        case Min(PushableColumnWithoutNestedColumn(name)) =>
          Some(buildAggregate("min", buildGeneric("ColumnReference", name)))
        case Max(PushableColumnWithoutNestedColumn(name)) =>
          Some(buildAggregate("max", buildGeneric("ColumnReference", name)))
        case count: Count if count.children.length == 1 =>
          count.children.head match {
            // SELECT COUNT(*) FROM table is translated to SELECT 1 FROM table
            case Literal(_, _) =>
              Some(buildCount("count", buildGeneric("Literal", "*"),
                              isDistinct = aggregate.isDistinct))
            case PushableColumnWithoutNestedColumn(name) =>
              Some(buildCount("count", buildGeneric("ColumnReference", name),
                              isDistinct = aggregate.isDistinct))
            case _ => None
          }
        case sum @ Sum(PushableColumnWithoutNestedColumn(name)) =>
          if (topAggregate) {
            Some(buildAggregate("sum", buildGeneric("ColumnReference", s"sum($name)")))
          } else {
            Some(buildAggregate("sum", buildGeneric("ColumnReference", name)))
          }
        case sum @ Sum(child: Expression) =>
          Some(buildAggregate("sum", buildAggExpr(child)))
        case _ => None
      }
    } else {
      None
    }
  }
  def buildGeneric(name: String, value: String): JsonObject = {
      val colRefBuilder = Json.createObjectBuilder()
      colRefBuilder.add(name, value)
      colRefBuilder.build()
  }
}
