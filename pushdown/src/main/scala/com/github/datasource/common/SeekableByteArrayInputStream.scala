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
// Adapted from
// https://github.com/apache/accumulo/blob/main/core/src/test/java
//       /org/apache/accumulo/core/file/rfile/RFileTest.java

package com.github.datasource.common

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PositionedReadable
import org.apache.hadoop.fs.Seekable
import org.apache.parquet.hadoop.util.HadoopStreams
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream

class SeekableByteArrayInputStream(buffer: Array[Byte])
  extends ByteArrayInputStream(buffer) with Seekable with PositionedReadable {

    override def getPos(): Long = {
        pos
    }
    override def seek(pos: Long): Unit = {
      if (mark != 0) {
        throw new IllegalStateException()
      }
      reset()
      var skipped = skip(pos)

      if (skipped != pos) {
        throw new IOException()
      }
    }
    def getCount(): Int = count
    override def seekToNewSource(targetPos: Long): Boolean = {
      false
    }

    override def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
      if (position >= buf.length) {
        throw new IllegalArgumentException()
      }
      if (position + length > buf.length) {
        throw new IllegalArgumentException()
      }
      if (length > buffer.length) {
        throw new IllegalArgumentException()
      }
      System.arraycopy(buf, position.asInstanceOf[Integer], buffer, offset, length)
      length
    }

    override def readFully(position: Long, buffer: Array[Byte]): Unit = {
      read(position, buffer, 0, buffer.length)
    }

    override def readFully(position: Long, buffer: Array[Byte], offset: Int, length: Int): Unit = {
      read(position, buffer, offset, length)
    }
}
