/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.twitter.lui

import java.util.Arrays

import org.apache.parquet.Log
import org.apache.parquet.column.ColumnWriteStore
import org.apache.parquet.column.ColumnWriter
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.column.values.dictionary.IntList
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType
import scala.collection.JavaConverters._
import com.twitter.lui.hadoop.{ ReadSupport, ReadContext }
import org.apache.hadoop.conf.Configuration

/**
 * Message level of the IO structure
 */
object MessageColumnIO {
  private val logger: Log = Log.getLog(getClass)
  private val DEBUG: Boolean = Log.DEBUG
}

case class MessageColumnIO(messageType: MessageType,
  validating: Boolean,
  createdBy: String,
  root: GroupColumnIO)