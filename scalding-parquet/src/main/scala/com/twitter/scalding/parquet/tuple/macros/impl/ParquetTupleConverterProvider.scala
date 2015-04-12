package com.twitter.scalding.parquet.tuple.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scalding.parquet.tuple.scheme._

import scala.reflect.macros.Context

object ParquetTupleConverterProvider {
  def toParquetTupleConverterImpl[T](ctx: Context)(implicit T: ctx.WeakTypeTag[T]): ctx.Expr[ParquetTupleConverter] = {
    import ctx.universe._

    if (!IsCaseClassImpl.isCaseClassType(ctx)(T.tpe))
      ctx.abort(ctx.enclosingPosition,
        s"""We cannot enforce ${T.tpe} is a case class,
            either it is not a case class or this macro call is possibly enclosed in a class.
            This will mean the macro is operating on a non-resolved type.""")

    def buildGroupConverter(tpe: Type, parentTree: Tree, isOption: Boolean, idx: Int, converterBodyTree: Tree,
      valueBuilder: Tree): Tree = {
      q"""new _root_.com.twitter.scalding.parquet.tuple.scheme.ParquetTupleConverter($parentTree){
            override def newConverter(i: Int): _root_.com.twitter.scalding.parquet.tuple.scheme.TupleFieldConverter[Any] = {
              $converterBodyTree
              throw new RuntimeException("invalid index: " + i)
            }

            override def createValue(): Any = {
              $valueBuilder
            }
          }"""
    }

    def matchField(idx: Int, fieldType: Type, isOption: Boolean): (Tree, Tree) = {

      def createConverter(converter: Tree): Tree = q"if($idx == i) return $converter"

      def primitiveFieldValue(converterType: Type): Tree = if (isOption) {
        val cachedRes = newTermName(ctx.fresh(s"fieldValue"))
        q"""
           {
              val $cachedRes = converters($idx).asInstanceOf[$converterType]
              if($cachedRes.hasValue) Some($cachedRes.currentValue) else _root_.scala.Option.empty[$fieldType]
           }
         """
      } else {
        q"converters($idx).asInstanceOf[$converterType].currentValue"
      }

      def primitiveFieldConverterAndFieldValue(converterType: Type): (Tree, Tree) = {
        val companion = converterType.typeSymbol.companionSymbol
        (createConverter(q"$companion(this)"), primitiveFieldValue(converterType))
      }

      def caseClassFieldValue: Tree = if (isOption) {
        val cachedRes = newTermName(ctx.fresh(s"fieldValue"))
        q"""
           {
              val $cachedRes = converters($idx)
              if($cachedRes.hasValue) Some($cachedRes.currentValue.asInstanceOf[$fieldType])
              else _root_.scala.Option.empty[$fieldType]
           }
         """
      } else {
        q"converters($idx).currentValue.asInstanceOf[$fieldType]"
      }

      fieldType match {
        case tpe if tpe =:= typeOf[String] =>
          primitiveFieldConverterAndFieldValue(typeOf[StringConverter])
        case tpe if tpe =:= typeOf[Boolean] =>
          primitiveFieldConverterAndFieldValue(typeOf[BooleanConverter])
        case tpe if tpe =:= typeOf[Byte] =>
          primitiveFieldConverterAndFieldValue(typeOf[ByteConverter])
        case tpe if tpe =:= typeOf[Short] =>
          primitiveFieldConverterAndFieldValue(typeOf[ShortConverter])
        case tpe if tpe =:= typeOf[Int] =>
          primitiveFieldConverterAndFieldValue(typeOf[IntConverter])
        case tpe if tpe =:= typeOf[Long] =>
          primitiveFieldConverterAndFieldValue(typeOf[LongConverter])
        case tpe if tpe =:= typeOf[Float] =>
          primitiveFieldConverterAndFieldValue(typeOf[FloatConverter])
        case tpe if tpe =:= typeOf[Double] =>
          primitiveFieldConverterAndFieldValue(typeOf[DoubleConverter])
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(idx, innerType, isOption = true)
        case tpe if IsCaseClassImpl.isCaseClassType(ctx)(tpe) =>
          val (innerConverters, innerFieldValues) = expandMethod(tpe).unzip
          val innerConverterTree = buildConverterBody(tpe, innerConverters)
          val innerValueBuilderTree = buildTupleValue(tpe, innerFieldValues)
          val innerGroupConverter = createConverter(buildGroupConverter(tpe, q"Option(this)", isOption, idx, innerConverterTree, innerValueBuilderTree))
          (innerGroupConverter, caseClassFieldValue)
        case _ => ctx.abort(ctx.enclosingPosition, s"Case class $T is not pure primitives or nested case classes")
      }
    }

    def expandMethod(outerTpe: Type): List[(Tree, Tree)] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .zipWithIndex
        .map {
          case (accessorMethod, idx) =>
            val fieldType = accessorMethod.returnType
            matchField(idx, fieldType, isOption = false)
        }.toList
    }

    def buildConverterBody(tpe: Type, trees: List[Tree]): Tree = {
      if (trees.isEmpty)
        ctx.abort(ctx.enclosingPosition, s"Case class $tpe has no primitive types we were able to extract")
      trees.foldLeft(q"") {
        case (existingTree, t) =>
          q"""$existingTree
              $t"""
      }
    }

    def buildTupleValue(tpe: Type, fieldValueBuilders: List[Tree]): Tree = {
      if (fieldValueBuilders.isEmpty)
        ctx.abort(ctx.enclosingPosition, s"Case class $tpe has no primitive types we were able to extract")
      val companion = tpe.typeSymbol.companionSymbol
      q"$companion(..$fieldValueBuilders)"
    }

    val (converters, fieldValues) = expandMethod(T.tpe).unzip
    val groupConverter = buildGroupConverter(T.tpe, q"None", isOption = false, -1, buildConverterBody(T.tpe, converters),
      buildTupleValue(T.tpe, fieldValues))

    ctx.Expr[ParquetTupleConverter](q"""
       $groupConverter
     """)
  }
}
