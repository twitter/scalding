package com.twitter.scalding.parquet.tuple.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl

import scala.reflect.macros.whitebox.Context

class ParquetSchemaProvider(fieldRenamer: (String => String)) {

  def toParquetSchemaImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[String] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    def matchField(fieldType: Type, originalFieldName: String, isOption: Boolean): Tree = {
      val fieldName = fieldRenamer(originalFieldName)
      val REPETITION_REQUIRED = q"_root_.org.apache.parquet.schema.Type.Repetition.REQUIRED"
      val REPETITION_OPTIONAL = q"_root_.org.apache.parquet.schema.Type.Repetition.OPTIONAL"
      val REPETITION_REPEATED = q"_root_.org.apache.parquet.schema.Type.Repetition.REPEATED"

      def repetition: Tree = if (isOption) REPETITION_OPTIONAL else REPETITION_REQUIRED

      def createPrimitiveTypeField(primitiveType: Tree): Tree =
        q"""new _root_.org.apache.parquet.schema.PrimitiveType($repetition, $primitiveType, $fieldName)"""

      fieldType match {
        case tpe if tpe =:= typeOf[String] =>
          createPrimitiveTypeField(q"_root_.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY")
        case tpe if tpe =:= typeOf[Boolean] =>
          createPrimitiveTypeField(q"_root_.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN")
        case tpe if tpe =:= typeOf[Short] || tpe =:= typeOf[Int] || tpe =:= typeOf[Byte] =>
          createPrimitiveTypeField(q"_root_.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32")
        case tpe if tpe =:= typeOf[Long] =>
          createPrimitiveTypeField(q"_root_.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64")
        case tpe if tpe =:= typeOf[Float] =>
          createPrimitiveTypeField(q"_root_.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT")
        case tpe if tpe =:= typeOf[Double] =>
          createPrimitiveTypeField(q"_root_.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(innerType, fieldName, isOption = true)
        case tpe if tpe.erasure =:= typeOf[List[Any]] || tpe.erasure =:= typeOf[Set[_]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          val innerFieldsType = matchField(innerType, "element", isOption = false)
          q"_root_.org.apache.parquet.schema.ConversionPatterns.listOfElements($repetition, $fieldName, $innerFieldsType)"
        case tpe if tpe.erasure =:= typeOf[Map[_, Any]] =>
          val List(keyType, valueType) = tpe.asInstanceOf[TypeRefApi].args
          val keyFieldType = matchField(keyType, "key", isOption = false)
          val valueFieldType = matchField(valueType, "value", isOption = false)
          q"_root_.org.apache.parquet.schema.ConversionPatterns.mapType($repetition, $fieldName, $keyFieldType, $valueFieldType)"
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) =>
          q"new _root_.org.apache.parquet.schema.GroupType($repetition, $fieldName, ..${expandMethod(tpe)})"
        case _ => c.abort(c.enclosingPosition, s"Case class $T has unsupported field type : $fieldType ")
      }
    }

    def expandMethod(outerTpe: Type): List[Tree] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map { accessorMethod =>
          val fieldName = accessorMethod.name.toTermName.toString
          val fieldType = accessorMethod.returnType
          matchField(fieldType, fieldName, isOption = false)
        }.toList
    }

    val expanded = expandMethod(T.tpe)

    if (expanded.isEmpty)
      c.abort(c.enclosingPosition, s"Case class $T.tpe has no fields we were able to extract")

    val messageTypeName = s"${T.tpe}".split("\\.").last
    val schema = q"""new _root_.org.apache.parquet.schema.MessageType($messageTypeName,
                        _root_.scala.Array.apply[_root_.org.apache.parquet.schema.Type](..$expanded):_*).toString"""

    c.Expr[String](schema)
  }
}
