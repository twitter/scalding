/*
Copyright 2015 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.db.macros

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.util.{ Success, Failure }

import com.twitter.scalding.db._
import com.twitter.scalding.db.macros.impl._
import com.twitter.scalding.db.macros.impl.handler._

object VerticaRowSerializerProvider {
  implicit def apply[T]: VerticaRowSerializer[T] = macro VerticaRowSerializerProviderImpl[T]
}

object VerticaRowSerializerProviderImpl {

  def apply[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[VerticaRowSerializer[T]] = {
    import c.universe._

    val columnFormats: List[ColumnFormat[c.type]] = ColumnDefinitionProviderImpl.getColumnFormats[T](c)

    val numColumns: Int = columnFormats.size

    val inputVal = newTermName(c.fresh("inputVal"))
    val outputStream = newTermName(c.fresh("outputStream"))
    val bitSet = newTermName(c.fresh("bitSet"))
    val bitSetBytes = newTermName(c.fresh("bitSetBytes"))
    val baos = newTermName(c.fresh("baos"))

    def buildSetter(fieldType: String, accessorTree: Tree) = {
      fieldType match {
        case "BIGINT" => q"""$baos.writeLong($accessorTree)"""
        case "TEXT" => q"""$baos.writeVarchar($accessorTree)"""
        case "VARCHAR" => q"""$baos.writeVarchar($accessorTree)"""
        case "BOOLEAN" => q"""$baos.writeBoolean($accessorTree)"""
        case "TINYINT" => q"""$baos.writeByte($accessorTree)"""
        case "SMALLINT" => q"""$baos.writeShort($accessorTree)"""
        case "INT" => q"""$baos.writeInt($accessorTree)"""
        case "DATE" => q"""$baos.writeDate($accessorTree)"""
        case "DATETIME" => q"""$baos.writeTimestampz($accessorTree)"""
        case "DOUBLE" => q"""$baos.writeDouble($accessorTree)"""
      }
    }

    val bitSetterAndMainSetter: List[(Tree, Tree)] = columnFormats.zipWithIndex.map {
      case (cf: ColumnFormat[_], indx: Int) =>

        val start: Tree = q"$inputVal"

        val accessor: Tree = cf.fieldAccessor.foldLeft(start) {
          case (existing: Tree, next: MethodSymbol) =>
            val t: Tree = Select(existing, next.name)
            t
        }

        if (cf.nullable) {
          val bitSetUpdater = q"""
            if(${Select(accessor, newTermName("isEmpty"))}) {
              $bitSet.setNull($indx)
            } else {
              $bitSet.setNotNull($indx)
            }
          """
          val mainUpdater = q"""
            if(${Select(accessor, newTermName("isDefined"))}) {
              ${buildSetter(cf.fieldType, Select(accessor, newTermName("get")))}
            }"""

          (bitSetUpdater, mainUpdater)
        } else {
          (q"", buildSetter(cf.fieldType, accessor))
        }
    }

    val bitSetters = bitSetterAndMainSetter.map(_._1)
    val mainUpdaters = bitSetterAndMainSetter.map(_._2)

    val res = q"""new _root_.com.twitter.scalding.db.VerticaRowSerializer[$T] {
              import _root_.com.twitter.scalding.db.LittleEndianJavaStreamEnrichments._
              def serialize($inputVal: $T, $outputStream: _root_.java.io.OutputStream): Unit = {
                val $baos = new _root_.java.io.ByteArrayOutputStream
                val $bitSet = _root_.com.twitter.scalding.db.VerticaBitSet($numColumns)
                ..$bitSetters
                val $bitSetBytes = $bitSet.getArray

                ..$mainUpdaters
                $outputStream.writeInt($baos.size)
                $outputStream.writeBytes($bitSetBytes)
                $baos.writeTo($outputStream)
              }
            }"""
    c.Expr[VerticaRowSerializer[T]](res)
  }
}
