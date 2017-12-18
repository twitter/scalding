package com.twitter.scalding.quotation

import scala.annotation.tailrec

case class Accessor(asString: String) extends AnyVal
case class TypeName(asString: String) extends AnyVal

sealed trait Projection {
  def andThen(accessor: Accessor, typeName: TypeName): Projection =
    Property(this, accessor, typeName)
}

/**
 * A reference of a type. If not nested within a `Property`, it means that all fields are used.
 */
final case class TypeReference(typeName: TypeName) extends Projection {
  override def toString = typeName.asString.split('.').last
}

/**
 * A projection property (e.g. `Person.name`)
 */
final case class Property(path: Projection, accessor: Accessor, typeName: TypeName) extends Projection {
  override def toString = s"$path.${accessor.asString}"
}

/**
 * Utility class to deal with a collection of projections.
 */
final class Projections private (val set: Set[Projection]) extends Serializable {

  /**
   * Returns the projections that are based on `tpe` and limits projections
   * to only properties that extend from `superClass`.
   */
  def of(typeName: TypeName, superClass: Class[_]): Projections = {

    def byType(p: Projection) = {
      @tailrec def loop(p: Projection): Boolean =
        p match {
          case TypeReference(`typeName`) => true
          case TypeReference(_) => false
          case Property(p, _, _) => loop(p)
        }
      loop(p)
    }

    def bySuperClass(p: Projection): Option[Projection] = {

      def isSubclass(c: TypeName) =
        try
          superClass.isAssignableFrom(Class.forName(c.asString))
        catch {
          case _: ClassNotFoundException =>
            false
        }

      def loop(p: Projection): Either[Projection, Option[Projection]] =
        p match {
          case TypeReference(tpe) =>
            Either.cond(!isSubclass(tpe), None, p)
          case p @ Property(path, name, tpe) =>
            loop(path) match {
              case Left(_) =>
                Either.cond(!isSubclass(tpe), Some(p), p)
              case Right(path) =>
                Right(path)
            }
        }

      loop(p) match {
        case Left(path) => Some(path)
        case Right(opt) => opt
      }
    }

    Projections(set.filter(byType).flatMap(bySuperClass))
  }

  /**
   * Given a set of base projections, returns the projections based on them.
   *
   * For instance, given a quoted function
   * 	 `val contact = Quoted.function { (c: Contact) => c.contact }`
   * and a call
   *   `(p: Person) => contact(p.name)`
   * returns the projection
   *   `Person.name.contact`
   */
  def basedOn(base: Set[Projection]): Projections = {
    def loop(base: Projection, p: Projection): Option[Projection] =
      p match {
        case TypeReference(tpe) =>
          base match {
            case TypeReference(`tpe`) => Some(base)
            case Property(_, _, `tpe`) => Some(base)
            case other => None
          }
        case Property(path, name, tpe) =>
          loop(base, path).map(Property(_, name, tpe))
      }
    Projections {
      set.flatMap { p =>
        base.flatMap(loop(_, p))
      }
    }
  }

  def ++(p: Projections) =
    Projections(set ++ p.set)

  override def toString =
    s"Projections(${set.mkString(", ")})"

  override def equals(other: Any) =
    other match {
      case other: Projections => set == other.set
      case other => false
    }

  override def hashCode =
    31 * set.hashCode
}

object Projections {
  val empty = apply(Set.empty)

  /**
   * Creates a normalized projections collection. For instance,
   * given two projections `Person.contact` and `Person.contact.phone`,
   * creates a collection with only `Person.contact`.
   */
  def apply(set: Set[Projection]) = {
    @tailrec def isNested(p: Projection): Boolean =
      p match {
        case Property(path, acessor, property) =>
          set.contains(path) || isNested(path)
        case _ => 
          false
      }
    new Projections(set.filter(!isNested(_)))
  }

  def flatten(list: Iterable[Projections]): Projections =
    list.foldLeft(empty)(_ ++ _)
}