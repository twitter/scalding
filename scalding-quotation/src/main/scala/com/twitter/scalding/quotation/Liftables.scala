package com.twitter.scalding.quotation

import scala.reflect.macros.blackbox.Context

/**
 * These Liftables allows us to lift values into quasiquote trees.
 * For example:
 *
 * def test(v: Source) => q"$v"
 *
 * uses `sourceLiftable`
 */
trait Liftables {
  val c: Context
  import c.universe.{ TypeName => _, _ }

  protected implicit val sourceLiftable: Liftable[Source] = Liftable {
    case Source(path, line) => q"_root_.com.twitter.scalding.quotation.Source($path, $line)"
  }

  protected implicit val projectionsLiftable: Liftable[Projections] = Liftable {
    case p => q"_root_.com.twitter.scalding.quotation.Projections(${p.set})"
  }

  protected implicit val typeNameLiftable: Liftable[TypeName] = Liftable {
    case TypeName(name) => q"_root_.com.twitter.scalding.quotation.TypeName($name)"
  }
  
  protected implicit val accessorLiftable: Liftable[Accessor] = Liftable {
    case Accessor(name) => q"_root_.com.twitter.scalding.quotation.Accessor($name)"
  }
  
  protected implicit val quotedLiftable: Liftable[Quoted] = Liftable {
    case Quoted(source, call, fa) => q"_root_.com.twitter.scalding.quotation.Quoted($source, $call, $fa)"
  }

  protected implicit val projectionLiftable: Liftable[Projection] = Liftable {
    case p: Property => q"$p"
    case p: TypeReference => q"$p"
  }

  protected implicit val propertyLiftable: Liftable[Property] = Liftable {
    case Property(path, accessor, tpe) => q"_root_.com.twitter.scalding.quotation.Property($path, $accessor, $tpe)"
  }

  protected implicit val typeReferenceLiftable: Liftable[TypeReference] = Liftable {
    case TypeReference(name) => q"_root_.com.twitter.scalding.quotation.TypeReference($name)"
  }
}
