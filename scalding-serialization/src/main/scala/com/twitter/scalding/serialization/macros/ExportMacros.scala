package com.twitter.scalding.serialization.macros

import com.twitter.scalding.serialization.Exported
import com.twitter.scalding.serialization.{OrderedSerialization, DefaultOrderedSerialization}
import scala.reflect.macros.whitebox

class ExportMacros(val c: whitebox.Context) {
  import c.universe._

  final def exportOrderedSerialization[D[x] <: DefaultOrderedSerialization[x], A](implicit
    D: c.WeakTypeTag[D[_]],
    A: c.WeakTypeTag[A]
  ): c.Expr[Exported[OrderedSerialization[A]]] = {
    val target = appliedType(D.tpe.typeConstructor, A.tpe)

    c.typecheck(q"implictly[$target]", silent = true) match {
      case EmptyTree => c.abort(c.enclosingPosition, s"Unable to infer value of type $target")
      case t => c.Expr[Exported[OrderedSerialization[A]]](
        q"new _root_.com.twitter.scalding.serialization.Exported($t: _root_.com.twitter.scalding.serialization.OrderedSerialization[$A])"
      )
    }
  }

}
