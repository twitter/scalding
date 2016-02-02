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
package com.twitter.lui.hadoop

import java.util.{ Map => JMap }

import org.apache.hadoop.conf.Configuration

import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.hadoop.api.InitContext
import com.twitter.lui.RecordReader
import org.apache.parquet.column.page.PageReadStore
import scala.reflect.ClassTag

object ReadSupport {

  /**
   * configuration key for a parquet read projection schema
   */
  val PARQUET_READ_SCHEMA: String = "parquet.read.schema"

  /**
   * attempts to validate and construct a {@link MessageType} from a read projection schema
   *
   * @param fileMessageType         the typed schema of the source
   * @param partialReadSchemaString the requested projection schema
   * @return the typed schema that should be used to read
   */
  def getSchemaForRead(fileMessageType: MessageType, partialReadSchemaString: String): MessageType = {
    if (partialReadSchemaString == null)
      fileMessageType
    else {
      val requestedMessageType: MessageType = MessageTypeParser.parseMessageType(partialReadSchemaString)
      getSchemaForRead(fileMessageType, requestedMessageType)
    }
  }

  def getSchemaForRead(fileMessageType: MessageType, projectedMessageType: MessageType): MessageType = {
    fileMessageType.checkContains(projectedMessageType)
    projectedMessageType
  }

}
abstract class ReadSupport[T] {

  private[this] var classTagHolder: ClassTag[T] = _

  protected final def classTag: ClassTag[T] = classTagHolder

  private[lui] def setClassTag(ct: ClassTag[T]) {
    classTagHolder = ct
  }

  def getRecordReader(
    columns: PageReadStore,
    configuration: Configuration,
    createdBy: String,
    fileMetadata: Map[String, String],
    fileSchema: MessageType,
    readContext: ReadContext,
    strictTypeChecking: Boolean): RecordReader[T]

  /**
   * called in {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)} in the front end
   *
   * @param context the initialisation context
   * @return the readContext that defines how to read the file
   */
  def init(context: InitContext): ReadContext

}
