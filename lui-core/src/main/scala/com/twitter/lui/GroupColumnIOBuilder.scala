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

import org.apache.parquet.schema.Type.Repetition.REPEATED
import org.apache.parquet.schema.Type.Repetition.REQUIRED

import java.util.Arrays
import scala.collection.mutable.{ Map => MMap }

import org.apache.parquet.Log
import org.apache.parquet.schema.GroupType

object LazyBox {
  def apply[T] = new LazyBox[T]
}
class LazyBox[T]() {
  private[this] var contents: Option[T] = None
  def get = this.synchronized {
    if (contents.isEmpty) sys.error("Accessed Box contents before populated")
    contents.get
  }
  def setValue(v: T): Unit = this.synchronized {
    if (contents.isDefined) sys.error("Attempted to set box contents twice!")
    contents = Some(v)
  }
}

case class GroupColumnIOBuilder(
  columnType: GroupType, index: Int) extends ColumnIOBuilder {
  private[this] var children = Vector[ColumnIOBuilder]()

  def add(child: ColumnIOBuilder) {
    children = children :+ child
  }

  override def setLevels(parentBox: LazyBox[GroupColumnIO],
    repetitionLevel: Int,
    definitionLevel: Int,
    fieldPath: Array[String],
    indexFieldPath: Array[Int],
    repetition: List[ColumnIOBuilder]): GroupColumnIO = {

    val ourBox = LazyBox[GroupColumnIO]

    val newChildren: Vector[ColumnIO] = children.map { child =>
      val newFieldPath: Array[String] = Arrays.copyOf(fieldPath, fieldPath.length + 1)
      val newIndexFieldPath: Array[Int] = Arrays.copyOf(indexFieldPath, indexFieldPath.length + 1)
      newFieldPath(fieldPath.length) = child.columnType.getName
      newIndexFieldPath(indexFieldPath.length) = child.index

      val newRepetition = if (child.columnType.isRepetition(REPEATED)) {
        repetition :+ child
      } else {
        repetition
      }

      child.setLevels(
        ourBox,
        // the type repetition level increases whenever there's a possible repetition
        if (child.columnType.isRepetition(REPEATED)) repetitionLevel + 1 else repetitionLevel,
        // the type definition level increases whenever a field can be missing (not required)
        if (!child.columnType.isRepetition(REQUIRED)) definitionLevel + 1 else definitionLevel,
        newFieldPath,
        newIndexFieldPath,
        newRepetition)
    }

    val childrenByName: Map[String, ColumnIO] = newChildren.map { child =>
      child.columnType.getName -> child
    }.toMap

    val gcIO = GroupColumnIO(
      columnType,
      parentBox,
      index,
      repetitionLevel,
      definitionLevel,
      fieldPath,
      indexFieldPath,
      childrenByName,
      newChildren)

    ourBox.setValue(gcIO)
    gcIO
  }
}
