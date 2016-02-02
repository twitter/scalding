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

/**
 * Primitive level of the IO structure
 *
 */
case class PrimitiveColumnIO(columnType: Type,
  parentBox: LazyBox[GroupColumnIO],
  id: Int,
  index: Int,
  repetitionLevel: Int,
  definitionLevel: Int,
  fieldPath: Array[String],
  indexFieldPath: Array[Int],
  columnDescriptor: ColumnDescriptor) extends ColumnIO {

  lazy val parent = parentBox.get

  lazy val path: Array[ColumnIO] = {
    @annotation.tailrec
    def go(p: ColumnIO, acc: List[ColumnIO]): List[ColumnIO] =
      if (p.parent != null) go(p.parent, p :: acc) else p :: acc

    go(this, Nil).toArray
  }

  override def getColumnNames: Seq[Seq[String]] = List(fieldPath)

  def getColumnDescriptor: ColumnDescriptor = columnDescriptor

  def isLast(r: Int): Boolean = getParent(r).last eq this

  def isFirst(r: Int): Boolean = getParent(r).first eq this

  override def last: PrimitiveColumnIO = this

  override def first: PrimitiveColumnIO = this

  def getPrimitive: PrimitiveTypeName = columnType.asPrimitiveType.getPrimitiveTypeName

  override def leaves: Iterator[PrimitiveColumnIO] = Iterator(this)
}
