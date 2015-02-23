package com.twitter.scalding.parquet.tuple.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl

import scala.reflect.macros.Context

object FieldValuesProvider {

  def toFieldValuesImpl[T](ctx: Context)(implicit T: ctx.WeakTypeTag[T]): ctx.Expr[T => Map[Int, Any]] = {
    import ctx.universe._

    if (!IsCaseClassImpl.isCaseClassType(ctx)(T.tpe))
      ctx.abort(ctx.enclosingPosition,
        s"""We cannot enforce ${T.tpe} is a case class,
            either it is not a case class or this macro call is possibly enclosed in a class.
            This will mean the macro is operating on a non-resolved type.""")

    def matchField(idx: Int, fieldType: Type, pTree: Tree): (Int, Tree) = {
      def appendFieldValue(idx: Int): (Int, Tree) =
        (idx + 1, q"""if($pTree != null) fieldValueMap += $idx -> $pTree""")

      fieldType match {
        case tpe if tpe =:= typeOf[String] ||
          tpe =:= typeOf[Boolean] ||
          tpe =:= typeOf[Short] ||
          tpe =:= typeOf[Int] ||
          tpe =:= typeOf[Long] ||
          tpe =:= typeOf[Float] ||
          tpe =:= typeOf[Double] =>
          appendFieldValue(idx)

        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val cacheName = newTermName(ctx.fresh(s"optionIndex"))
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          val (newIdx, subTree) = matchField(idx, innerType, q"$cacheName")
          (newIdx, q"""
               if($pTree.isDefined) {
                  val $cacheName = $pTree.get
                  $subTree
               }
             """)

        case tpe if IsCaseClassImpl.isCaseClassType(ctx)(tpe) => expandMethod(idx, tpe, pTree)
        case _ => ctx.abort(ctx.enclosingPosition, s"Case class $T is not pure primitives or nested case classes")
      }
    }

    def expandMethod(parentIdx: Int, outerTpe: Type, pTree: Tree): (Int, Tree) = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .foldLeft((parentIdx, q"")) {
          case ((idx, existingTree), accessorMethod) =>
            val (newIdx, subTree) = matchField(idx, accessorMethod.returnType, q"""$pTree.$accessorMethod""")
            (newIdx, q"""
              $existingTree
              $subTree""")
        }
    }

    val (finalIdx, allFieldValues) = expandMethod(0, T.tpe, q"t")

    if (finalIdx == 0)
      ctx.abort(ctx.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")

    val fieldValues = q"""
         val values: $T => _root_.scala.collection.immutable.Map[Int, Any] = t => {
            var fieldValueMap = _root_.scala.collection.immutable.Map[Int, Any]()
            $allFieldValues
            fieldValueMap
         }
         values
      """
    ctx.Expr[T => Map[Int, Any]](fieldValues)
  }
}
