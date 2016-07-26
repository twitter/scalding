package com.twitter.scalding.parquet.tuple.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scalding.parquet.tuple.scheme._

import scala.reflect.macros.whitebox.Context

class ParquetReadSupportProvider(schemaProvider: ParquetSchemaProvider) {

  private[this] sealed trait CollectionType
  private[this] case object NOT_A_COLLECTION extends CollectionType
  private[this] case object OPTION extends CollectionType
  private[this] case object LIST extends CollectionType
  private[this] case object SET extends CollectionType
  private[this] case object MAP extends CollectionType

  def toParquetReadSupportImpl[T](ctx: Context)(implicit T: ctx.WeakTypeTag[T]): ctx.Expr[ParquetReadSupport[T]] = {
    import ctx.universe._

    if (!IsCaseClassImpl.isCaseClassType(ctx)(T.tpe))
      ctx.abort(ctx.enclosingPosition,
        s"""We cannot enforce ${T.tpe} is a case class,
            either it is not a case class or this macro call is possibly enclosed in a class.
            This will mean the macro is operating on a non-resolved type.""")

    def buildGroupConverter(tpe: Type, converters: List[Tree], converterGetters: List[Tree],
      converterResetCalls: List[Tree], valueBuilder: Tree): Tree =
      q"""new _root_.com.twitter.scalding.parquet.tuple.scheme.ParquetTupleConverter[$tpe]{
            ..$converters

            override def currentValue: $tpe = $valueBuilder

            override def getConverter(i: Int): _root_.org.apache.parquet.io.api.Converter = {
              ..$converterGetters
              throw new RuntimeException("invalid index: " + i)
            }

            override def reset(): Unit = {
              ..$converterResetCalls
            }

      }"""

    def matchField(idx: Int, fieldType: Type, collectionType: CollectionType): (Tree, Tree, Tree, Tree) = {
      def fieldConverter(converterName: TermName, converter: Tree, isPrimitive: Boolean = false): Tree = {
        def primitiveCollectionElementConverter: Tree =
          q"""override val child: _root_.com.twitter.scalding.parquet.tuple.scheme.TupleFieldConverter[$fieldType] =
                new _root_.com.twitter.scalding.parquet.tuple.scheme.CollectionElementPrimitiveConverter[$fieldType](this) {
                  override val delegate: _root_.com.twitter.scalding.parquet.tuple.scheme.PrimitiveFieldConverter[$fieldType] = $converter
                }
          """

        def caseClassFieldCollectionElementConverter: Tree =
          q"""override val child: _root_.com.twitter.scalding.parquet.tuple.scheme.TupleFieldConverter[$fieldType] =
                new _root_.com.twitter.scalding.parquet.tuple.scheme.CollectionElementGroupConverter[$fieldType](this) {
                  override val delegate: _root_.com.twitter.scalding.parquet.tuple.scheme.TupleFieldConverter[$fieldType] = $converter
                }
          """

        collectionType match {
          case OPTION =>
            val child = if (isPrimitive) primitiveCollectionElementConverter else caseClassFieldCollectionElementConverter
            q"""
              val $converterName = new _root_.com.twitter.scalding.parquet.tuple.scheme.OptionConverter[$fieldType] {
                $child
              }
            """
          case LIST =>
            val child = if (isPrimitive) primitiveCollectionElementConverter else caseClassFieldCollectionElementConverter
            q"""
              val $converterName = new _root_.com.twitter.scalding.parquet.tuple.scheme.ListConverter[$fieldType] {
                $child
              }
            """
          case SET =>
            val child = if (isPrimitive) primitiveCollectionElementConverter else caseClassFieldCollectionElementConverter

            q"""
              val $converterName = new _root_.com.twitter.scalding.parquet.tuple.scheme.SetConverter[$fieldType] {
                $child
              }
            """
          case MAP => converter
          case _ => q"val $converterName = $converter"
        }

      }

      def createMapFieldConverter(converterName: TermName, K: Type, V: Type, keyConverter: Tree,
        valueConverter: Tree): Tree =
        q"""val $converterName = new _root_.com.twitter.scalding.parquet.tuple.scheme.MapConverter[$K, $V] {

              override val child: _root_.com.twitter.scalding.parquet.tuple.scheme.TupleFieldConverter[($K, $V)] =
                new _root_.com.twitter.scalding.parquet.tuple.scheme.MapKeyValueConverter[$K, $V](this) {
                   override val keyConverter: _root_.com.twitter.scalding.parquet.tuple.scheme.TupleFieldConverter[$K] = $keyConverter
                   override val valueConverter: _root_.com.twitter.scalding.parquet.tuple.scheme.TupleFieldConverter[$V] = $valueConverter
                }
            }
         """

      def createFieldMatchResult(converterName: TermName, converter: Tree): (Tree, Tree, Tree, Tree) = {
        val converterGetter: Tree = q"if($idx == i) return $converterName"
        val converterResetCall: Tree = q"$converterName.reset()"
        val converterFieldValue: Tree = q"$converterName.currentValue"
        (converter, converterGetter, converterResetCall, converterFieldValue)
      }

      def matchPrimitiveField(converterType: Type): (Tree, Tree, Tree, Tree) = {
        val converterName = newTermName(ctx.fresh(s"fieldConverter"))
        val innerConverter: Tree = q"new $converterType()"
        val converter: Tree = fieldConverter(converterName, innerConverter, isPrimitive = true)
        createFieldMatchResult(converterName, converter)
      }

      def matchCaseClassField(groupConverter: Tree): (Tree, Tree, Tree, Tree) = {
        val converterName = newTermName(ctx.fresh(s"fieldConverter"))
        val converter: Tree = fieldConverter(converterName, groupConverter)
        createFieldMatchResult(converterName, converter)
      }

      def matchMapField(K: Type, V: Type, keyConverter: Tree, valueConverter: Tree): (Tree, Tree, Tree, Tree) = {
        val converterName = newTermName(ctx.fresh(s"fieldConverter"))
        val mapConverter = createMapFieldConverter(converterName, K, V, keyConverter, valueConverter)
        createFieldMatchResult(converterName, mapConverter)
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
          matchField(idx, innerType, OPTION)
        case tpe if tpe.erasure =:= typeOf[List[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(idx, innerType, LIST)
        case tpe if tpe.erasure =:= typeOf[Set[_]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(idx, innerType, SET)
        case tpe if tpe.erasure =:= typeOf[Map[_, Any]] =>
          val List(keyType, valueType) = tpe.asInstanceOf[TypeRefApi].args
          val (keyConverter, _, _, _) = matchField(0, keyType, MAP)
          val (valueConverter, _, _, _) = matchField(0, valueType, MAP)
          matchMapField(keyType, valueType, keyConverter, valueConverter)
        case tpe if IsCaseClassImpl.isCaseClassType(ctx)(tpe) =>
          val (innerConverters, innerConvertersGetters, innerConvertersResetCalls, innerFieldValues) = unzip(expandMethod(tpe))
          val innerValueBuilderTree = buildTupleValue(tpe, innerFieldValues)
          val converterTree: Tree = buildGroupConverter(tpe, innerConverters, innerConvertersGetters,
            innerConvertersResetCalls, innerValueBuilderTree)
          matchCaseClassField(converterTree)
        case _ => ctx.abort(ctx.enclosingPosition, s"Case class $T has unsupported field type : $fieldType ")
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
            matchField(idx, fieldType, NOT_A_COLLECTION)
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
    val groupConverter = buildGroupConverter(T.tpe, converters, converterGetters, convertersResetCalls,
      buildTupleValue(T.tpe, fieldValues))

    val schema = schemaProvider.toParquetSchemaImpl[T](ctx)
    val readSupport = q"""
      new _root_.com.twitter.scalding.parquet.tuple.scheme.ParquetReadSupport[$T]($schema) {
        override val tupleConverter: _root_.com.twitter.scalding.parquet.tuple.scheme.ParquetTupleConverter[$T] = $groupConverter
      }
    """
    ctx.Expr[ParquetReadSupport[T]](readSupport)
  }
}
