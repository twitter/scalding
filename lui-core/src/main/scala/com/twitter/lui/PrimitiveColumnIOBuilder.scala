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

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

case class PrimitiveColumnIOBuilder(columnType: Type, index: Int, val id: Int) extends ColumnIOBuilder {
  override def setLevels(parentBox: LazyBox[GroupColumnIO],
    repetitionLevel: Int,
    definitionLevel: Int,
    fieldPath: Array[String],
    indexFieldPath: Array[Int],
    repetition: List[ColumnIOBuilder]): PrimitiveColumnIO = {
    val pType: PrimitiveType = columnType.asPrimitiveType
    val columnDescriptor = new ColumnDescriptor(
      fieldPath,
      pType.getPrimitiveTypeName,
      pType.getTypeLength,
      repetitionLevel,
      definitionLevel)

    PrimitiveColumnIO(
      columnType,
      parentBox,
      id,
      index,
      repetitionLevel,
      definitionLevel,
      fieldPath,
      indexFieldPath,
      columnDescriptor)
  }
}
