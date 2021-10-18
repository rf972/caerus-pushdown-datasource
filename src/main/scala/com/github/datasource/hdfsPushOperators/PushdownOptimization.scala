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

import java.io.FileWriter
import java.util
import java.util.HashMap

import scala.collection.mutable
import scala.util.{Either, Left => EitherLeft, Right => EitherRight}

import com.github.datasource.common.{PushdownJson, PushdownJsonStatus}
import org.json._
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.types._

object PushdownOptimizationRule extends Rule[LogicalPlan] {
  protected val sparkSession = SparkSession.builder
    .getOrCreate()
  var ruleCount = 0
  private def mapAttribute(origExpression: Any,
                           newProject: Seq[NamedExpression]) : Any = {
    origExpression match {
      case a @ Alias(child, name) =>
        new Alias(mapAttribute(child, newProject).asInstanceOf[Expression],
                  name)(a.exprId, a.qualifier, a.explicitMetadata, a.nonInheritableMetadataKeys)
      case Cast(expression, dataType, timeZoneId) =>
        new Cast(mapAttribute(expression, newProject).asInstanceOf[NamedExpression],
                 dataType, timeZoneId)
      case AttributeReference(name, dataType, nullable, meta) =>
        newProject.find(_.name == name).get
      case default => throw new Exception(s"Unknown: ${default}")
    }
  }
  private def convertProject(origProject: Seq[NamedExpression],
                             newProject: Seq[NamedExpression]): Seq[NamedExpression] = {
    val linkedProject = origProject.map {x =>
      mapAttribute(x, newProject).asInstanceOf[NamedExpression]
    }
    linkedProject
  }
  private def checkJson(operation: String) : Unit = {
    try {
     val jsonObject = new JSONObject(operation)
     logger.info("Processor found: " + jsonObject.getString("processor"))
     val schema = jsonObject.getJSONArray("schema")
     for (i <- 0 until schema.length) {
       val field = schema.getJSONObject(i)
       logger.info("name: " + field.getString("name") +
                   " type: " + field.getString("type"))
     }
    } catch {
      case err: JSONException =>
        logger.error("Error " + err.toString())
    }
  }
  private def getAttributeValues(origExpression: Any) : (String, String) = {
    origExpression match {
      case a @ Alias(child, name) =>
        getAttributeValues(child)
      case Cast(expression, dataType, timeZoneId) =>
        getAttributeValues(expression)
      case AttributeReference(name, dataType, nullable, meta) =>
        (name, dataType.toString)
      case default => throw new Exception(s"Unknown: ${default}")
    }
  }
  private def convertProjectOp(project: Seq[NamedExpression]) = {
    val projectJsonArray = new JSONArray()
    val projectJson = project.foreach {x =>
      val attribute = getAttributeValues(x)
      val attrJson = new JSONObject()
      attrJson.put("name", attribute._1)
      attrJson.put("type", attribute._2)
      projectJsonArray.put(attrJson)
    }
    projectJsonArray
  }
  private def createJsonOp(project: Seq[NamedExpression]): String = {
    val jsonObj = new JSONObject()
    jsonObj.put("processor", "lambda")
    val projectJson = convertProjectOp(project)
    jsonObj.put("schema", projectJson)
    checkJson(jsonObj.toString)
    jsonObj.toString
  }
  private def getAttribute(origExpression: Any) : Either[String, AttributeReference] = {
    origExpression match {
      case Alias(child, name) =>
        getAttribute(child)
      case Cast(expression, dataType, timeZoneId) =>
        getAttribute(expression)
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        EitherRight(attrib)
      case default => EitherLeft("Unknown Attribute: " + default)
    }
  }
  private def getAttributeReferences(project: Seq[NamedExpression]):
              Either[String, Seq[AttributeReference]] = {
    var failed = false
    val attributes = project.flatMap {x =>
      getAttribute(x) match {
        case EitherLeft(l) => logger.info(l)
          failed = true
          Seq[AttributeReference]()
        case EitherRight(r) =>
          Seq(r)
      }
    }
    if (failed) {
      EitherLeft("Failed attribute references.")
    } else {
      EitherRight(attributes)
    }
  }
  private def getJsonSchema(params: String) : StructType = {
    var newSchema: StructType = new StructType()
    try {
      val jsonObject = new JSONObject(params)
      logger.info("Processor found: " + jsonObject.getString("processor"))
      val schemaJson = jsonObject.getJSONArray("schema")
      for (i <- 0 until schemaJson.length()) {
        val field = schemaJson.getJSONObject(i)
        val dataType = field.getString("type") match {
          case "StringType" => StringType
          case "IntegerType" => IntegerType
          case "DoubleType" => DoubleType
          case "LongType" => LongType
        }
        newSchema = newSchema.add(field.getString("name"), dataType, true)
      }
    } catch {
      case err: JSONException =>
        logger.error("Error " + err.toString())
    }
    newSchema
  }

  def getFilterAttributes(filters: Seq[Expression]): Either[String, Seq[AttributeReference]] = {
    var failed = false
    val attributes = filters.flatMap(f => {
      val attrSeq = getFilterExpressionAttributes(f)
      if (attrSeq.length == 0) {
        failed = true
      }
      attrSeq
    })
    if (failed) {
      EitherLeft("Failed getting filter expr attributes")
    } else {
      EitherRight(attributes)
    }
  }
  def getFilterExpressionAttributes(filter: Expression): Seq[AttributeReference] = {
    filter match {
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        Seq(attrib)
      case Or(left, right) => getFilterExpressionAttributes(left) ++
                              getFilterExpressionAttributes(right)
      case And(left, right) => getFilterExpressionAttributes(left) ++
                               getFilterExpressionAttributes(right)
      case Not(filter) => getFilterExpressionAttributes(filter)
      case EqualTo(attr, value) => getFilterExpressionAttributes(attr)
      case LessThan(attr, value) => getFilterExpressionAttributes(attr)
      case GreaterThan(attr, value) => getFilterExpressionAttributes(attr)
      case LessThanOrEqual(attr, value) => getFilterExpressionAttributes(attr)
      case GreaterThanOrEqual(attr, value) => getFilterExpressionAttributes(attr)
      case IsNull(attr) => getFilterExpressionAttributes(attr)
      case IsNotNull(attr) => getFilterExpressionAttributes(attr)
      case StartsWith(left, right) => getFilterExpressionAttributes(left)
      case EndsWith(left, right) => getFilterExpressionAttributes(left)
      case Contains(left, right) => getFilterExpressionAttributes(left)
      case other@_ => logger.info("unknown filter:" + other) ; Seq[AttributeReference]()
    }
  }
  private def needsRule(rel: DataSourceV2ScanRelation): Boolean = {
    val scan = rel match {
      case DataSourceV2ScanRelation(relation, scan, output) =>
        scan
    }
    (!scan.isInstanceOf[HdfsOpScan])
  }
  def canHandlePlan(project: Seq[NamedExpression],
                    filters: Seq[Expression],
                    child: DataSourceV2ScanRelation): Boolean = {
    val relationArgs = child match {
      case DataSourceV2ScanRelation(relation, scan, output) =>
      (relation, scan, output)
    }
    val scanArgs = relationArgs._2 match {
      case ParquetScan(_, _, _, dataSchema, readSchema, _, _, opts, _, _) =>
        (dataSchema, readSchema, opts)
      /* case HdfsOpScan(schema, schema, opts, _) =>
        (schema, opts) */
    }
    if (scanArgs._1 == scanArgs._2) {
      logger.warn("Plan not modified. No Project Necessary. " +
                  (scanArgs._3).get("currenttest"))
      return false
    }
    val attrReferencesEither = getAttributeReferences(project)
    if (attrReferencesEither.isLeft) {
      logger.warn("Plan not modified due to project" +
                  (scanArgs._3).get("currenttest"))
      false
    } else {
      val filterReferencesEither = getFilterAttributes(filters)
      if (filterReferencesEither.isLeft) {
        logger.warn("Plan not modified due to filter" +
                    (scanArgs._3).get("currenttest"))
        false
      } else {
        true
      }
    }
  }
  private def getNdpRelation(path: String): Option[DataSourceV2Relation] = {
    val df = sparkSession.read
          .format("pushdownOp")
          .option("format", "parquet")
          .option("outputFormat", "binary")
          .load(path)
    val logicalPlan = df.queryExecution.optimizedPlan
    logicalPlan match {
      case s@ScanOperation(project,
                           filters,
                           child: DataSourceV2ScanRelation) =>
        child match {
          case DataSourceV2ScanRelation(relation, scan, output) =>
             Some(relation)
          case _ => None
        }
      case _ => None
    }
  }
  private def transformProject(project: Seq[NamedExpression],
                               filters: Seq[Expression],
                               child: DataSourceV2ScanRelation): LogicalPlan = {
    val relationArgs = child match {
      case DataSourceV2ScanRelation(relation, scan, output) =>
      (relation, scan, output)
    }
    val attrReferencesEither = getAttributeReferences(project)

    val attrReferences = attrReferencesEither match {
      case EitherRight(r) => r
      case EitherLeft(l) => Seq[AttributeReference]()
    }
    val scanArgs = relationArgs._2 match {
      case ParquetScan(_, _, _, dataSchema, _, _, _, opts, _, _) =>
        (dataSchema, opts)
      case HdfsOpScan(schema, opts) =>
        (schema, opts)
    }
    val filterReferencesEither = getFilterAttributes(filters)
    val filterReferences = filterReferencesEither match {
      case EitherRight(r) => r
      case EitherLeft(l) => Seq[AttributeReference]()
    }
    val opt = new HashMap[String, String](scanArgs._2)
    val path = opt.get("path").replaceFirst("hdfs://.*:9000/", "ndphdfs://dikehdfs/")
    val ndpRel = getNdpRelation(path)
    val compression = opt.getOrDefault("ndpcompression", "None")
    val compLevel = opt.getOrDefault("ndpcomplevel", "-100")
    val test = opt.getOrDefault("currenttest", "Unknown Test")
    opt.put("path", path)
    opt.put("format", "parquet")
    opt.put("outputFormat", "binary")
    logger.info("Using compression: " + compression + " Level: " + compLevel)
    var filtersStatus = PushdownJson.validateFilters(filters)
    /* The below disables all filter pushdown */
    // filtersStatus = PushdownJsonStatus.Invalid
    var references = {
      filtersStatus match {
        case PushdownJsonStatus.Invalid =>
          (attrReferences ++ filterReferences).distinct
        case PushdownJsonStatus.PartiallyValid =>
          (attrReferences ++ filterReferences).distinct
        case PushdownJsonStatus.FullyValid =>
          attrReferences.distinct
      }
    }
    var cols = references.toStructType.fields.map(x => s"" + s"${x.name}").mkString(",")
    /* The below allows us to log the available filters
     * for pushdown, even if we currently do not push these down.
     * These get logged to filters.txt, along with the
     * projects and the Spark view of the filters too.
     */
    if (false) {
      val filtersJson = PushdownJson.getFiltersJsonMaxDesired(filters, test)
      val fw = new FileWriter("/build/filters.txt", true)
      try {
        fw.write("Pushdown " + opt.getOrDefault("currenttest", "") +
                " Filters " + filters.mkString(", ") + "\n")
        fw.write("Pushdown " + opt.getOrDefault("currenttest", "") +
                " Projects " + cols + "\n")
        fw.write("Pushdown " + opt.getOrDefault("currenttest", "") +
                " Filter Json " + filtersJson + "\n")
      }
      finally fw.close()
    }
    if (filtersStatus != PushdownJsonStatus.Invalid) {
      val filtersJson = PushdownJson.getFiltersJson(filters, test)
      logger.warn("Pushdown " + opt.getOrDefault("currenttest", "") +
                  " Filter Json " + filtersJson)
      opt.put("ndpjsonfilters", filtersJson)
    } else {
      logger.warn("No Pushdown " + filters.toString)
    }
    opt.put("ndpprojectcolumns", cols)
    val hdfsScanObject = new HdfsOpScan(references.toStructType, opt)
    val scanRelation = DataSourceV2ScanRelation(ndpRel.get, hdfsScanObject, references)
    val filterCondition = filters.reduceLeftOption(And)
    val withFilter = {
      if (filtersStatus == PushdownJsonStatus.FullyValid) {
        /* Clip the filter from the DAG, since we are going to
         * push down the entire filter to NDP.
         */
        scanRelation
      } else {
        filterCondition.map(LogicalFilter(_, scanRelation)).getOrElse(scanRelation)
      }
    }
    if (withFilter.output != project || filters.length == 0) {
      Project(project, withFilter)
    } else {
      withFilter
    }
  }
  private def pushFilterProject(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan.transform {
      case s@ScanOperation(project,
                           filters,
                           child: DataSourceV2ScanRelation) if (needsRule(child) &&
                           canHandlePlan(project, filters, child)) =>
        ruleCount += 1
        logger.info("rules processed: %d".format(ruleCount))
        val modified = transformProject(project, filters, child)
        logger.info("before scan: \n" + project + "\n" + s)
        logger.info("after scan: \n" + modified)
        modified
    }
    if (newPlan != plan) {
      logger.info("before: \n" + plan)
      logger.info("after: \n" + newPlan)
    }
    newPlan
  }
  private def transformAggregate(groupingExpressions: Seq[Expression],
                                 aggregateExpressions: Seq[NamedExpression],
                                 child: LogicalPlan): LogicalPlan = {
    val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
    var ordinal = 0
    val aggregates = aggregateExpressions.flatMap { expr =>
      expr.collect {
        // Do not push down duplicated aggregate expressions. For example,
        // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
        // `max(a)` to the data source.
        case agg: AggregateExpression
            if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
          aggExprToOutputOrdinal(agg.canonicalized) = ordinal
          ordinal += 1
          agg
      }
    }
    val schema = new StructType()
    val newOutput = schema.map(f => AttributeReference(f.name, f.dataType,
                                                       f.nullable, f.metadata)())
    assert(newOutput.length == groupingExpressions.length + aggregates.length)
    val groupAttrs = groupingExpressions.zip(newOutput).map {
      case (a: Attribute, b: Attribute) => b.withExprId(a.exprId)
      case (_, b) => b
    }
    val output = groupAttrs ++ newOutput.drop(groupAttrs.length)

    /* logInfo(
      s"""
          |Pushing operators to ${sHolder.relation.name}
          |Pushed Aggregate Functions:
          | ${pushedAggregates.get.aggregateExpressions.mkString(", ")}
          |Pushed Group by:
          | ${pushedAggregates.get.groupByColumns.mkString(", ")}
          |Output: ${output.mkString(", ")}
          """.stripMargin) */
    val relationArgs = child match {
      case DataSourceV2ScanRelation(relation, scan, output) =>
      (relation, scan, output)
    }
    val scanArgs = relationArgs._2 match {
      case ParquetScan(_, _, _, dataSchema, _, _, _, opts, _, _) =>
        (dataSchema, opts)
      case HdfsOpScan(schema, opts) =>
        (schema, opts)
    }
    val opt = new HashMap[String, String](scanArgs._2)
    val hdfsScanObject = new HdfsOpScan(output.toStructType, opt)
    val scanRelation = DataSourceV2ScanRelation(relationArgs._1,
                                                hdfsScanObject, output)
    val plan = Aggregate(
                  output.take(groupingExpressions.length), aggregateExpressions, scanRelation)
    plan
  }
  private def pushAggregate(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan.transform {
      case aggNode @ Aggregate(groupingExpressions, resultExpressions, childAgg) =>
        childAgg match {
          case s@ScanOperation(project,
               filters,
               child: DataSourceV2ScanRelation)
            if filters.isEmpty =>
              transformAggregate(groupingExpressions, resultExpressions, child)
            aggNode
          case r: DataSourceV2ScanRelation =>
            aggNode
          case other =>
            aggNode
        }
    }
    if (newPlan != plan) {
      logger.info("before: \n" + plan)
      logger.info("after: \n" + newPlan)
    }
    newPlan
  }
  protected val logger = LoggerFactory.getLogger(getClass)
  def apply(inputPlan: LogicalPlan): LogicalPlan = {
    // val after = pushAggregate(pushFilterProject(inputPlan))
    val after = pushFilterProject(inputPlan)
    after
  }
}
