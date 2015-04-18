package com.twitter.scalding.parquet.tuple.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scalding.parquet.tuple.scheme._

import scala.reflect.macros.Context

object ParquetTupleConverterProvider {
  def toParquetTupleConverterImpl[T](ctx: Context)(implicit T: ctx.WeakTypeTag[T]): ctx.Expr[ParquetTupleConverter[T]] = {
    import ctx.universe._

    if (!IsCaseClassImpl.isCaseClassType(ctx)(T.tpe))
      ctx.abort(ctx.enclosingPosition,
        s"""We cannot enforce ${T.tpe} is a case class,
            either it is not a case class or this macro call is possibly enclosed in a class.
            This will mean the macro is operating on a non-resolved type.""")

    def buildGroupConverter(tpe: Type, isOption: Boolean, converters: List[Tree], converterGetters: List[Tree],
      converterResetCalls: List[Tree], valueBuilder: Tree): Tree = {
      q"""new _root_.com.twitter.scalding.parquet.tuple.scheme.ParquetTupleConverter[$tpe]{
            ..$converters

            override def currentValue: $tpe = $valueBuilder

            override def getConverter(i: Int): _root_.parquet.io.api.Converter = {
              ..$converterGetters
              throw new RuntimeException("invalid index: " + i)
            }

            override def reset(): Unit = {
              ..$converterResetCalls
            }

          }"""
    }

    def matchField(idx: Int, fieldType: Type, isOption: Boolean): (Tree, Tree, Tree, Tree) = {

      def createPrimitiveConverter(converterName: TermName, converterType: Type): Tree = {
        if (isOption) {
          q"""
             val $converterName = new _root_.com.twitter.scalding.parquet.tuple.scheme.OptionalPrimitiveFieldConverter[$fieldType] {
               override val delegate: _root_.com.twitter.scalding.parquet.tuple.scheme.PrimitiveFieldConverter[$fieldType] = new $converterType()
             }
           """
        } else {
          q"val $converterName = new $converterType()"
        }
      }

      def createCaseClassFieldConverter(converterName: TermName, groupConverter: Tree): Tree = {
        if (isOption) {
          q"""
             val $converterName = new _root_.com.twitter.scalding.parquet.tuple.scheme.OptionalParquetTupleConverter[$fieldType] {
               override val delegate: _root_.com.twitter.scalding.parquet.tuple.scheme.ParquetTupleConverter[$fieldType] = $groupConverter
             }
           """
        } else {
          q"val $converterName = $groupConverter"
        }
      }

      def createFieldMatchResult(converterName: TermName, converter: Tree): (Tree, Tree, Tree, Tree) = {
        val converterGetter: Tree = q"if($idx == i) return $converterName"
        val converterResetCall: Tree = q"$converterName.reset()"
        val converterFieldValue: Tree = q"$converterName.currentValue"
        (converter, converterGetter, converterResetCall, converterFieldValue)
      }

      def matchPrimitiveField(converterType: Type): (Tree, Tree, Tree, Tree) = {
        val converterName = newTermName(ctx.fresh(s"fieldConverter"))
        val converter: Tree = createPrimitiveConverter(converterName, converterType)
        createFieldMatchResult(converterName, converter)
      }

      def matchCaseClassField(groupConverter: Tree): (Tree, Tree, Tree, Tree) = {
        val converterName = newTermName(ctx.fresh(s"fieldConverter"))
        val converter: Tree = createCaseClassFieldConverter(converterName, groupConverter)
        createFieldMatchResult(converterName, converter)
      }

      fieldType match {
        case tpe if tpe =:= typeOf[String] =>
          matchPrimitiveField(typeOf[StringConverter])
        case tpe if tpe =:= typeOf[Boolean] =>
          matchPrimitiveField(typeOf[BooleanConverter])
        case tpe if tpe =:= typeOf[Byte] =>
          matchPrimitiveField(typeOf[ByteConverter])
        case tpe if tpe =:= typeOf[Short] =>
          matchPrimitiveField(typeOf[ShortConverter])
        case tpe if tpe =:= typeOf[Int] =>
          matchPrimitiveField(typeOf[IntConverter])
        case tpe if tpe =:= typeOf[Long] =>
          matchPrimitiveField(typeOf[LongConverter])
        case tpe if tpe =:= typeOf[Float] =>
          matchPrimitiveField(typeOf[FloatConverter])
        case tpe if tpe =:= typeOf[Double] =>
          matchPrimitiveField(typeOf[DoubleConverter])
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(idx, innerType, isOption = true)
        case tpe if IsCaseClassImpl.isCaseClassType(ctx)(tpe) =>
          val (innerConverters, innerConvertersGetters, innerConvertersResetCalls, innerFieldValues) = unzip(expandMethod(tpe))
          val innerValueBuilderTree = buildTupleValue(tpe, innerFieldValues)
          val converterTree: Tree = buildGroupConverter(tpe, isOption, innerConverters,
            innerConvertersGetters, innerConvertersResetCalls, innerValueBuilderTree)
          matchCaseClassField(converterTree)
        case _ => ctx.abort(ctx.enclosingPosition, s"Case class $T is not pure primitives or nested case classes")
      }
    }

    def expandMethod(outerTpe: Type): List[(Tree, Tree, Tree, Tree)] =
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .zipWithIndex
        .map {
          case (accessorMethod, idx) =>
            val fieldType = accessorMethod.returnType
            matchField(idx, fieldType, isOption = false)
        }.toList

    def unzip(treeTuples: List[(Tree, Tree, Tree, Tree)]): (List[Tree], List[Tree], List[Tree], List[Tree]) = {
      val emptyTreeList = List[Tree]()
      treeTuples.foldRight(emptyTreeList, emptyTreeList, emptyTreeList, emptyTreeList) {
        case ((t1, t2, t3, t4), (l1, l2, l3, l4)) =>
          (t1 :: l1, t2 :: l2, t3 :: l3, t4 :: l4)
      }
    }

    def buildTupleValue(tpe: Type, fieldValueBuilders: List[Tree]): Tree = {
      if (fieldValueBuilders.isEmpty)
        ctx.abort(ctx.enclosingPosition, s"Case class $tpe has no primitive types we were able to extract")
      val companion = tpe.typeSymbol.companionSymbol
      q"$companion(..$fieldValueBuilders)"
    }

    val (converters, converterGetters, convertersResetCalls, fieldValues) = unzip(expandMethod(T.tpe))
    val groupConverter = buildGroupConverter(T.tpe, isOption = false, converters, converterGetters,
      convertersResetCalls, buildTupleValue(T.tpe, fieldValues))

    ctx.Expr[ParquetTupleConverter[T]](q"""
       $groupConverter
     """)
  }
}
