package com.twitter.scalding.typed

import scala.math.Ordering

object KeyGroupingMacro {
  import reflect.macros.Context

  def apply[T](c: Context)(
    implicit T: c.WeakTypeTag[T]): c.Expr[KeyGrouping[T]] = {
    import c.mirror._
    import c.universe._

    val ordTypeTag = implicitly[c.WeakTypeTag[Ordering[T]]]
    val implicitInScope = c.inferImplicitValue(ordTypeTag.tpe)

    if (implicitInScope.toString() == "<empty>")
      throw new IllegalArgumentException("Just can't")

    val pos = c.macroApplication.pos

    val desc = ordSerDesc(implicitInScope.toString())
      .getOrElse(implicitInScope.toString())
    val path =
      if (pos.source.path.contains("/workspace/"))
        pos.source.path.substring(pos.source.path.indexOf("/workspace/"))
      else
        pos.source.path

    val info = s">>> ordering | ${sanitize(path)} | ${pos.line} | " +
      s"${sanitize(T.tpe.toString())} | ${sanitize(desc)} <<<"
    println(info)

    val call =
      q"""com.twitter.scalding.typed.KeyGrouping[$T]($implicitInScope)"""
    c.Expr[KeyGrouping[T]](call)
  }

  def sanitize(s: String): String =
    s.replace(">>>", "!!!")
      .replace("<<<", "!!!")
      .replace("|", "!")
      .replace('\n', ' ')

  def ordSerDesc(desc: String): Option[String] = {
    val str =
      "com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.MacroEqualityOrderedSerialization["
    if (desc.contains(str)) {
      val tmp = desc.substring(desc.indexOf(str) + str.length)
      Some("ordered serialization for: " + tmp.substring(0, tmp.indexOf("]")))
    } else {
      None
    }
  }
}
