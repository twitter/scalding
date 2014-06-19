package com.twitter.scalding

import scala.annotation.tailrec

/**
 * @author Mansur Ashraf
 * @since 2/9/14.
 */
trait Partitioner[A, B] {

  def partition(b: B): Option[(A, B)]

  def unfoldr(seed: B): TraversableOnce[A] = {
    @tailrec
    def unfoldr(seed: B, result: Seq[A]): Seq[A] = partition(seed) match {
      case None => result
      case Some((a, b)) => unfoldr(b, a +: result)
    }
    unfoldr(seed, Seq[A]())
  }

}
