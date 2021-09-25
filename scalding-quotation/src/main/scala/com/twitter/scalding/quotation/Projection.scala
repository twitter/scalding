package com.twitter.scalding.quotation

import scala.annotation.tailrec

case class Accessor(asString: String) extends AnyVal
case class TypeName(asString: String) extends AnyVal

sealed trait Projection {
  def andThen(accessor: Accessor, typeName: TypeName): Projection =
    Property(this, accessor, typeName)

  def rootProjection: TypeReference = {
    @tailrec def loop(p: Projection): TypeReference =
      p match {
        case p @ TypeReference(_) => p
        case Property(p, _, _) => loop(p)
      }
    loop(this)
  }

  /**
   * Given a base projection, returns the projection based on it if applicable.
   *
   * For instance, given a quoted function
   * 	 `val contact = Quoted.function { (c: Contact) => c.contact }`
   * and a call
   *   `(p: Person) => contact(p.name)`
   * produces the projection
   *   `Person.name.contact`
   */
  def basedOn(base: Projection): Option[Projection] =
    this match {
      case TypeReference(tpe) =>
        base match {
          case TypeReference(`tpe`) => Some(base)
          case Property(_, _, `tpe`) => Some(base)
          case other => None
        }
      case Property(path, name, tpe) =>
        path.basedOn(base).map(Property(_, name, tpe))
    }

  /**
   * Limits projections to only values of `superClass`. Example:
   *
   * case class Person(name: String, contact: Contact) extends ThriftObject
   * case class Contact(phone: Phone) extends ThriftObject
   * case class Phone(number: String)
   *
   * For the super class `ThriftObject`, it produces the transformations:
   *
   * Person.contact.phone        => Some(Person.contact.phone)
   * Person.contact.phone.number => Some(Person.contact.phone)
   * Person.name.isEmpty         => Some(Person.name)
   * Phone.number								 => None
   */
  def bySuperClass(superClass: Class[_]): Option[Projection] = {

    def isSubclass(c: TypeName) =
      try
        superClass.isAssignableFrom(Class.forName(c.asString))
      catch {
        case _: ClassNotFoundException =>
          false
      }

    def loop(p: Projection): Either[Projection, Option[Projection]] =
      p match {
        case TypeReference(typeName) =>
          Either.cond(!isSubclass(typeName), None, p)
        case p @ Property(path, name, typeName) =>
          loop(path) match {
            case Left(_) =>
              Either.cond(!isSubclass(typeName), Some(p), p)
            case Right(path) =>
              Right(path)
          }
      }

    loop(this) match {
      case Left(path) => Some(path)
      case Right(opt) => opt
    }
  }
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
   * Returns the projections that are based on `typeName` and limits projections
   * to only properties that extend from `superClass`.
   */
  def of(typeName: TypeName, superClass: Class[_]): Projections =
    Projections {
      set.filter(_.rootProjection.typeName == typeName)
        .flatMap(_.bySuperClass(superClass))
    }

  def basedOn(base: Set[Projection]): Projections =
    Projections {
      set.flatMap { p =>
        base.flatMap(p.basedOn)
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