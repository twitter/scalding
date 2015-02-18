package com.twitter.scalding.parquet.tuple.macros.impl

import scala.language.experimental.macros
import com.twitter.bijection.macros.impl.IsCaseClassImpl
import parquet.schema.MessageType

import scala.reflect.macros.Context

object SchemaProviderImpl {

  def toParquetSchemaImp[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[MessageType] = {

    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    case class Extractor(tpe: Type, toTree: Tree)
    case class Builder(toTree: Tree = q"")

    implicit val builderLiftable = new Liftable[Builder] {
      def apply(b: Builder): Tree = b.toTree
    }

    implicit val extractorLiftable = new Liftable[Extractor] {
      def apply(b: Extractor): Tree = b.toTree
    }

    val REPETITION_REQUIRED = q"ParquetType.Repetition.REQUIRED"
    val REPETITION_OPTIONAL = q"ParquetType.Repetition.OPTIONAL"

    def createPrimitiveTypeField(isOption: Boolean, primitiveType: Tree, fieldName: String): List[Tree] =
      List(q"""new PrimitiveType(${getRepetition(isOption)}, $primitiveType, $fieldName)""")

    def getRepetition(isOption: Boolean): Tree = if (isOption) REPETITION_OPTIONAL else REPETITION_REQUIRED

    def matchField(fieldType: Type, outerName: String, fieldName: String, isOption: Boolean): List[Tree] = {
      val parquetFieldName = s"$outerName$fieldName"
      fieldType match {
        case tpe if tpe =:= typeOf[String] =>
          createPrimitiveTypeField(isOption, q"PrimitiveType.PrimitiveTypeName.BINARY", parquetFieldName)
        case tpe if tpe =:= typeOf[Boolean] =>
          createPrimitiveTypeField(isOption, q"PrimitiveType.PrimitiveTypeName.BOOLEAN", parquetFieldName)
        case tpe if tpe =:= typeOf[Short] || tpe =:= typeOf[Int] =>
          createPrimitiveTypeField(isOption, q"PrimitiveType.PrimitiveTypeName.INT32", parquetFieldName)
        case tpe if tpe =:= typeOf[Long] =>
          createPrimitiveTypeField(isOption, q"PrimitiveType.PrimitiveTypeName.INT64", parquetFieldName)
        case tpe if tpe =:= typeOf[Float] =>
          createPrimitiveTypeField(isOption, q"PrimitiveType.PrimitiveTypeName.FLOAT", parquetFieldName)
        case tpe if tpe =:= typeOf[Double] =>
          createPrimitiveTypeField(isOption, q"PrimitiveType.PrimitiveTypeName.DOUBLE", parquetFieldName)
        case tpe if tpe.erasure =:= typeOf[Option[Any]] && isOption =>
          c.abort(c.enclosingPosition, s"Nested options do not make sense being mapped onto a tuple fields in cascading.")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(innerType, outerName, fieldName, true)
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => expandMethod(tpe, s"$parquetFieldName.", isOption = false)
        case _ => c.abort(c.enclosingPosition, s"Case class $T is not pure primitives or nested case classes")
      }
    }

    def expandMethod(outerTpe: Type, outerName: String, isOption: Boolean): List[Tree] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .flatMap { accessorMethod =>
          val fieldName = accessorMethod.name.toTermName.toString
          val fieldType = accessorMethod.returnType
          matchField(fieldType, outerName, fieldName, false)
        }.toList
    }

    def expandCaseClass(outerTpe: Type, outerName: String, isOption: Boolean): Tree = {
      val expanded = expandMethod(outerTpe, outerName, isOption)
      if (expanded.isEmpty) c.abort(c.enclosingPosition, s"Case class $outerTpe has no primitive types we were able to extract")
      val messageTypeName = s"${outerTpe}".split("\\.").last
      q"""import parquet.schema.{MessageType, PrimitiveType, Type => ParquetType}
          new MessageType($messageTypeName, Array.apply[ParquetType](..$expanded):_*)"""
    }

    c.Expr[MessageType](expandCaseClass(T.tpe, "", isOption = false))
  }
}
