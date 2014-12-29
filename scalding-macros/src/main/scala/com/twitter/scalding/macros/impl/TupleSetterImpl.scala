package com.twitter.scalding.macros.impl

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.bijection.macros.impl.IsCaseClassImpl
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object TupleSetterImpl {
  def caseClassTupleSetterNoProof[T]: TupleSetter[T] = macro caseClassTupleSetterNoProofImpl[T]

  def caseClassTupleSetterImpl[T](c: Context)(proof: c.Expr[IsCaseClass[T]])(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] =
    caseClassTupleSetterNoProofImpl(c)(T)

  def caseClassTupleSetterNoProofImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] = {
    import c.universe._

    def matchField(outerTpe: Type, idx: Int, pTree: Tree): (Int, Tree) = {
      def simpleType(accessor: Tree) =
        (idx + 1, q"""${accessor}(${idx}, $pTree)""")

      outerTpe match {
        case tpe if tpe =:= typeOf[String] => simpleType(q"tup.setString")
        case tpe if tpe =:= typeOf[Boolean] => simpleType(q"tup.setBoolean")
        case tpe if tpe =:= typeOf[Short] => simpleType(q"tup.setShort")
        case tpe if tpe =:= typeOf[Int] => simpleType(q"tup.setInteger")
        case tpe if tpe =:= typeOf[Long] => simpleType(q"tup.setLong")
        case tpe if tpe =:= typeOf[Float] => simpleType(q"tup.setFloat")
        case tpe if tpe =:= typeOf[Double] => simpleType(q"tup.setDouble")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val cacheName = newTermName(s"optionIdx$idx")
          val (newIdx, subTree) =
            matchField(tpe.asInstanceOf[TypeRefApi].args.head, idx, q"$cacheName")
          val nullSetters = (idx until newIdx).map { curIdx =>
            q"""tup.set($curIdx, null)"""
          }

          (newIdx, q"""
            if($pTree.isDefined) {
              val $cacheName = $pTree.get
              $subTree
            } else {
              ..$nullSetters
            }
            """)

        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => expandMethod(tpe, idx, pTree)
        case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives, Option of a primitive nested case classes")
      }
    }

    def expandMethod(outerTpe: Type, parentIdx: Int, pTree: Tree): (Int, Tree) =
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .foldLeft((parentIdx, q"")) {
          case ((idx, existingTree), accessorMethod) =>
            val (newIdx, subTree) = matchField(accessorMethod.returnType, idx, q"""$pTree.$accessorMethod""")
            (newIdx, q"""
              $existingTree
              $subTree""")
        }

    val (finalIdx, set) = expandMethod(T.tpe, 0, q"t")

    val res = q"""
    new _root_.com.twitter.scalding.TupleSetter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override def apply(t: $T): _root_.cascading.tuple.Tuple = {
        val tup = _root_.cascading.tuple.Tuple.size($finalIdx)
        $set
        tup
      }
      override val arity: scala.Int = $finalIdx
    }
    """
    c.Expr[TupleSetter[T]](res)
  }
}
