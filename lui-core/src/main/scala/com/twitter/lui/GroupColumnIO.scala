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

import org.apache.parquet.Log
import org.apache.parquet.schema.GroupType

/**
 * Group level of the IO structure
 *
 *
 */

object GroupColumnIO {
  private val logger: Log = Log.getLog(getClass)
  private val DEBUG: Boolean = Log.DEBUG
}

case class GroupColumnIO(columnType: GroupType,
  parentBox: LazyBox[GroupColumnIO],
  index: Int,
  repetitionLevel: Int,
  definitionLevel: Int,
  fieldPath: Array[String],
  indexFieldPath: Array[Int],
  childrenByName: Map[String, ColumnIO],
  children: Vector[ColumnIO]) extends ColumnIO {

  lazy val parent = parentBox.get

  import GroupColumnIO._

  override def getColumnNames: Seq[Seq[String]] = children.flatMap(_.getColumnNames)

  override def last: PrimitiveColumnIO = children.last.last

  override def first: PrimitiveColumnIO = children.head.first

  def getChild(name: String): Option[ColumnIO] = childrenByName.get(name)

  def getChild(fieldIndex: Int): ColumnIO =
    try {
      children(fieldIndex)
    } catch {
      case e: IndexOutOfBoundsException =>
        throw new org.apache.parquet.io.InvalidRecordException(s"could not get child $fieldIndex from $children", e)
    }

  override def leaves: Iterator[PrimitiveColumnIO] = children.toIterator.flatMap(_.leaves)

}
