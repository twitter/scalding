package com.twitter.scalding.typed

import com.stripe.dagon.HMap
import java.io.Serializable
import scala.util.hashing.MurmurHash3

/**
 * This class is an like a higher kinded PartialFunction
 * which we use to look up sources and sinks in a safe
 * way
 */
abstract class Resolver[I[_], O[_]] extends Serializable {
  def apply[A](i: I[A]): Option[O[A]]

  def orElse(that: Resolver[I, O]): Resolver[I, O] =
    Resolver.orElse(this, that)

  def andThen[O2[_]](that: Resolver[O, O2]): Resolver[I, O2] =
    Resolver.AndThen(this, that)
}

object Resolver extends Serializable {
  private case class HMapResolver[I[_], O[_]](toHMap: HMap[I, O]) extends Resolver[I, O] {
    override val hashCode = toHMap.hashCode

    def apply[A](i: I[A]): Option[O[A]] = toHMap.get(i)
  }

  private case class OrElse[I[_], O[_]](first: Resolver[I, O], second: Resolver[I, O]) extends Resolver[I, O] {
    override val hashCode: Int = MurmurHash3.productHash(this)

    def apply[A](i: I[A]): Option[O[A]] = {
      @annotation.tailrec
      def lookup(from: Resolver[I, O], rest: List[Resolver[I, O]]): Option[O[A]] =
        from match {
          case OrElse(first, second) =>
            lookup(first, second :: rest)
          case notOrElse =>
            notOrElse(i) match {
              case some @ Some(_) => some
              case None =>
                rest match {
                  case Nil => None
                  case h :: tail =>
                    lookup(h, tail)
                }
            }
        }

      lookup(first, second :: Nil)
    }
  }

  private case class AndThen[X[_], Y[_], Z[_]](first: Resolver[X, Y], second: Resolver[Y, Z]) extends Resolver[X, Z] {
    override val hashCode: Int = MurmurHash3.productHash(this)

    def apply[A](i: X[A]): Option[Z[A]] =
      first(i).flatMap(second(_))
  }

  def empty[I[_], O[_]]: Resolver[I, O] =
    HMapResolver(HMap.empty[I, O])

  def pair[I[_], O[_], A](input: I[A], output: O[A]): Resolver[I, O] =
    HMapResolver[I, O](HMap.empty[I, O] + (input -> output))

  def fromHMap[I[_], O[_]](hmap: HMap[I, O]): Resolver[I, O] =
    HMapResolver(hmap)

  def orElse[I[_], O[_]](first: Resolver[I, O], second: Resolver[I, O]): Resolver[I, O] =
    first match {
      case same if same == second => same
      case hmp @ HMapResolver(fhm) =>
        second match {
          case HMapResolver(shm) =>
            // dagon does not have a ++ :(
            val merged = fhm.keySet.foldLeft(shm) { (hmap, k) =>
              def addKey[A](k: I[A]): HMap[I, O] = {
                hmap + (k -> fhm(k))
              }
              addKey(k)
            }
            HMapResolver(merged)
          case notHMap =>
            OrElse(hmp, notHMap)
        }
      case OrElse(a, b) =>
        // Make sure we are right associated
        orElse(a, orElse(b, second))
      case notOrElse => OrElse(notOrElse, second)
    }
}
