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
package org.apache.spark.api.python.generic

import java.io._
import java.net._
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path => JavaFiles}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import net.razorvine.pickle.Unpickler
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.{TaskContext, _}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonBroadcast, PythonFunction, _}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{BUFFER_SIZE, EXECUTOR_CORES}
import org.apache.spark.internal.config.Python._
import org.apache.spark.resource.ResourceProfile.{EXECUTOR_CORES_LOCAL_PROPERTY, PYSPARK_MEMORY_LOCAL_PROPERTY}
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.types._
import org.apache.spark.util._

class GenericPythonRunner(function: Array[Byte], dag: String) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val context: TaskContext = org.apache.spark.TaskContext.get()

  def eval: ReaderIteratorExBase[Array[Byte]] = {
    val workerEnv = new java.util.HashMap[String, String]()
    workerEnv.put("PYTHONHASHSEED", "0")
    // val curDir = System.getProperty("user.dir")
    // logger.info(s"current working directory: ${curDir}")
    val rootPath = SparkFiles.getRootDirectory() + "/pydike_venv"
    val func = PythonFunction(
      command = function,
      envVars = workerEnv,
      pythonIncludes = List.empty[String].asJava,
      pythonExec = s"${rootPath}/bin/python",
      pythonVer = "3.8",
      broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
      accumulator = null)
    val funcs = Seq(ChainedPythonFunctions(Seq(func)))
    val outputIterator = new PythonRunner(funcs, dag)
      .compute(List.empty.iterator, context.partitionId(), context)
    outputIterator
  }
}
