package com.twitter.scalding.parquet.tuple.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl

import scala.reflect.macros.Context

object ParquetSchemaProvider {
  def toParquetSchemaImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[String] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    def matchField(fieldType: Type, fieldName: String, isOption: Boolean): List[Tree] = {
      val REPETITION_REQUIRED = q"_root_.parquet.schema.Type.Repetition.REQUIRED"
      val REPETITION_OPTIONAL = q"_root_.parquet.schema.Type.Repetition.OPTIONAL"

      def repetition: Tree = if (isOption) REPETITION_OPTIONAL else REPETITION_REQUIRED

      def createPrimitiveTypeField(primitiveType: Tree): List[Tree] =
        List(q"""new _root_.parquet.schema.PrimitiveType($repetition, $primitiveType, $fieldName)""")

      fieldType match {
        case tpe if tpe =:= typeOf[String] =>
          createPrimitiveTypeField(q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY")
        case tpe if tpe =:= typeOf[Boolean] =>
          createPrimitiveTypeField(q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN")
        case tpe if tpe =:= typeOf[Short] || tpe =:= typeOf[Int] || tpe =:= typeOf[Byte] =>
          createPrimitiveTypeField(q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32")
        case tpe if tpe =:= typeOf[Long] =>
          createPrimitiveTypeField(q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64")
        case tpe if tpe =:= typeOf[Float] =>
          createPrimitiveTypeField(q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT")
        case tpe if tpe =:= typeOf[Double] =>
          createPrimitiveTypeField(q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(innerType, fieldName, isOption = true)
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) =>
          List(q"""new _root_.parquet.schema.GroupType($repetition, $fieldName,
                        _root_.scala.Array.apply[_root_.parquet.schema.Type](..${expandMethod(tpe)}):_*)""")
        case _ => c.abort(c.enclosingPosition, s"Case class $T is not pure primitives or nested case classes")
      }
    }

    def expandMethod(outerTpe: Type): List[Tree] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .flatMap { accessorMethod =>
          val fieldName = accessorMethod.name.toTermName.toString
          val fieldType = accessorMethod.returnType
          matchField(fieldType, fieldName, isOption = false)
        }.toList
    }

    val expanded = expandMethod(T.tpe)

    if (expanded.isEmpty)
      c.abort(c.enclosingPosition, s"Case class $T.tpe has no primitive types we were able to extract")

    val messageTypeName = s"${T.tpe}".split("\\.").last
    val schema = q"""new _root_.parquet.schema.MessageType($messageTypeName,
                        _root_.scala.Array.apply[_root_.parquet.schema.Type](..$expanded):_*).toString"""

    c.Expr[String](schema)
  }
}
