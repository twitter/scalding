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
 */
object StableKnownDirectSubclasses {

  def apply(c: Context)(tpe: c.Type): List[c.universe.TypeSymbol] = 
    tpe.typeSymbol.asClass.knownDirectSubclasses.map(_.asType).toList.sortBy(_.fullName)
}
