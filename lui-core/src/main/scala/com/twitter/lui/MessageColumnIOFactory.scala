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

import com.twitter.lui.hadoop.{ ReadSupport, ReadContext }
import java.util.ArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.TypeVisitor
import scala.collection.JavaConverters._

/**
 * Factory constructing the ColumnIO structure from the schema
 *
 */

object MessageColumnIOFactory {

  def getRecordReader[T](
    columns: PageReadStore,
    configuration: Configuration,
    createdBy: String,
    fileMetadata: Map[String, String],
    fileSchema: MessageType,
    readContext: ReadContext,
    readSupport: ReadSupport[T],
    strictTypeChecking: Boolean): RecordReader[T] =
    readSupport.getRecordReader(columns, configuration, createdBy, fileMetadata, fileSchema, readContext, strictTypeChecking)

  case class StrictTypeChecking(on: Boolean)
  case class LeafIndex(var toInt: Int) {
    def getAndIncr: Int = {
      val old = toInt
      toInt = old + 1
      old
    }
  }

  def buildMessageColumnIO(
    readContext: ReadContext,
    fileSchema: MessageType,
    createdBy: String,
    strictTypeChecking: Boolean): MessageColumnIO = {
    val messageBuilder = MessageColumnIOBuilder(readContext.requestedSchema, false, createdBy)

    implicit val stC = StrictTypeChecking(strictTypeChecking)
    implicit val leafIndex = LeafIndex(0)

    visitChildren(messageBuilder.rootColumnIOBuilder, fileSchema, readContext.requestedSchema.asGroupType)

    messageBuilder.setLevels
  }

  private[this] def visitNode(parentBuilder: GroupColumnIOBuilder,
    tpe: Type,
    currentRequestedType: Type,
    currentRequestedIndex: Int)(implicit strictTypeChecking: StrictTypeChecking, leafIndex: LeafIndex): Unit = {
    tpe match {
      case groupType: GroupType =>
        if (currentRequestedType.isPrimitive) {
          incompatibleSchema(groupType, currentRequestedType)
        }
        val newGrpIOBuilder = GroupColumnIOBuilder(groupType, currentRequestedIndex)
        parentBuilder.add(newGrpIOBuilder)
        visitChildren(newGrpIOBuilder, groupType, currentRequestedType.asGroupType)

      case primitiveType: PrimitiveType =>
        if (!currentRequestedType.isPrimitive ||
          (strictTypeChecking.on &&
            currentRequestedType.asPrimitiveType.getPrimitiveTypeName != primitiveType.getPrimitiveTypeName)) {
          incompatibleSchema(primitiveType, currentRequestedType)
        }
        val newIO = PrimitiveColumnIOBuilder(primitiveType, currentRequestedIndex, leafIndex.getAndIncr)
        parentBuilder.add(newIO)
    }

  }

  private[this] def visitChildren(groupIOBuilder: GroupColumnIOBuilder, groupType: GroupType, requestedGroupType: GroupType)(implicit strictTypeChecking: StrictTypeChecking, leafIndex: LeafIndex): Unit =
    groupType.getFields.asScala.foreach { curTpe =>
      // if the file schema does not contain the field it will just stay null
      if (requestedGroupType.containsField(curTpe.getName)) {

        val currentRequestedIndex: Int = requestedGroupType.getFieldIndex(curTpe.getName)
        val currentRequestedType: Type = requestedGroupType.getType(currentRequestedIndex)

        if (currentRequestedType.getRepetition.isMoreRestrictiveThan(curTpe.getRepetition)) {
          incompatibleSchema(curTpe, currentRequestedType)
        }
        visitNode(groupIOBuilder, curTpe, currentRequestedType, currentRequestedIndex)
      }
    }

  private[this] def incompatibleSchema(fileType: Type, requestedType: Type): Unit = {
    throw new ParquetDecodingException("The requested schema is not compatible with the file schema. incompatible types: " + requestedType + " != " + fileType)
  }

}
