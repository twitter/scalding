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
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Type.Repetition

abstract class ColumnIO {
  def columnType: Type
  def parent: GroupColumnIO
  def index: Int

  def fieldPath: Array[String]

  def getFieldPath(level: Int): String = fieldPath(level)

  def indexFieldPath: Array[Int]

  def getIndexFieldPath(level: Int): Int = indexFieldPath(level)

  def name: String = columnType.getName

  def repetitionLevel: Int

  def definitionLevel: Int

  // def setLevels(r: Int, d: Int, fieldPath: Array[String], indexFieldPath: Array[Int], repetition: List[ColumnIO], path: List[ColumnIO]) {
  //   setRepetitionLevel(r)
  //   setDefinitionLevel(d)
  //   this.fieldPath = fieldPath
  //   this.indexFieldPath = indexFieldPath
  // }

  def getColumnNames: Seq[Seq[String]]

  def last: PrimitiveColumnIO
  def first: PrimitiveColumnIO

  def leaves: Iterator[PrimitiveColumnIO]

  def getParent(r: Int): ColumnIO =
    if (repetitionLevel == r && columnType.isRepetition(Repetition.REPEATED)) {
      this
    } else if (parent != null && parent.definitionLevel >= r) {
      parent.getParent(r)
    } else {
      throw new org.apache.parquet.io.InvalidRecordException(s"no parent($r) for $fieldPath")
    }

  override def toString: String =
    s"${getClass.getSimpleName} $name r: $repetitionLevel d: $definitionLevel $fieldPath"

}
