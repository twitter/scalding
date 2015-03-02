package com.twitter.scalding_internal.db.vertica.macros

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.util.{ Success, Failure }

import com.twitter.scalding_internal.db.{ ColumnDefinition, ColumnDefinitionProvider, ResultSetExtractor }
import com.twitter.scalding_internal.db.macros._
import com.twitter.scalding_internal.db.macros.impl.handler._
import com.twitter.scalding_internal.db.macros.impl.ColumnDefinitionProviderImpl
import com.twitter.scalding_internal.db.vertica.VerticaRowSerializer

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

    val res = q"""new _root_.com.twitter.scalding_internal.db.vertica.VerticaRowSerializer[$T] {
              import _root_.com.twitter.scalding_internal.db.vertica.LittleEndianJavaStreamEnrichments._
              def serialize($inputVal: $T, $outputStream: _root_.java.io.OutputStream): Unit = {
                val $baos = new _root_.java.io.ByteArrayOutputStream
                val $bitSet = _root_.com.twitter.scalding_internal.db.vertica.VerticaBitSet($numColumns)
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
