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

import org.json._
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.expressions._

object PushdownJson {

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
      case other@_ => logger.warn("unknown filter:" + other)
         buildUnknown(other)
    }
  }
  def getFiltersJson(filters: Seq[Expression]): String = {
    val filterBuilder = Json.createObjectBuilder()
    filterBuilder.add("Name", "Filters")
    val filterArrayBuilder = Json.createArrayBuilder()
    for (f <- filters) {
        val j = buildFiltersJson(f)
        filterArrayBuilder.add(j)
    }
    filterBuilder.add("FilterArray", filterArrayBuilder.build())

    val stringWriter = new StringWriter()
    val writer = Json.createWriter(stringWriter)
    writer.writeObject(filterBuilder.build())
    writer.close()
    val jsonString = stringWriter.getBuffer().toString()
    val indented = (new JSONObject(jsonString)).toString(4)
    indented
  }
  def validateFilterExpression(expr: Expression): Boolean = {
    expr match {
      case Or(left, right) => validateFilterExpression(left) &&
                              validateFilterExpression(right)
      case And(left, right) => validateFilterExpression(left) &&
                              validateFilterExpression(right)
      case Not(filter) => validateFilterExpression(filter)
      case EqualTo(left, right) => validateFilterExpression(left) &&
                                   validateFilterExpression(right)
      case LessThan(left, right) => validateFilterExpression(left) &&
                                     validateFilterExpression(right)
      case GreaterThan(left, right) => validateFilterExpression(left) &&
                                       validateFilterExpression(right)
      case LessThanOrEqual(left, right) => validateFilterExpression(left) &&
                                           validateFilterExpression(right)
      case GreaterThanOrEqual(left, right) => validateFilterExpression(left) &&
                                              validateFilterExpression(right)
      case IsNull(attr) => validateFilterExpression(attr)
      case IsNotNull(attr) => validateFilterExpression(attr)
      case StartsWith(left, right) => validateFilterExpression(left) &&
                                    validateFilterExpression(right)
      case EndsWith(left, right) => validateFilterExpression(left) &&
                                    validateFilterExpression(right)
      case Contains(left, right) => validateFilterExpression(left) &&
                                    validateFilterExpression(right)
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        true
      case Literal(value, dataType) =>
        true
      case other@_ => logger.warn("unknown filter:" + other)
        false
    }
  }
  def validateFilters(filters: Seq[Expression]): Boolean = {
    var status: Boolean = true
    for (f <- filters) {
      if (!validateFilterExpression(f)) {
        status = false
      }
    }
    status
  }
}
