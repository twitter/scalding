package com.twitter.scalding.serialization.macros.impl.ordered_serialization.providers

import scala.reflect.macros.whitebox.Context

/**
 * The `knownDirectSubclasses` method doesn't provide stable ordering
 * since it returns an unordered `Set` and the `Type` AST nodes don't
 * override the `hashCode` method, relying on the default identity
 * `hashCode`.
 *
 * This function makes the ordering stable using a list ordered by the
 * full name of the types.
 *
 * Note: because of an implementation detail, immutable Scala sets
 * preserve the insertion ordering for up to four elements (see `Set1`,
 * `Set2`, etc). This method maintains the same behavior for backward
 * compatibility.
 */
object StableKnownDirectSubclasses {

  def apply(c: Context)(tpe: c.Type): List[c.universe.TypeSymbol] = {
    import c.universe._

    def sort(types: List[TypeSymbol]) =
      if (types.size <= 4)
        types
      else
        types.sortBy(_.fullName)

    sort(tpe.typeSymbol.asClass.knownDirectSubclasses.map(_.asType).toList)
  }
}