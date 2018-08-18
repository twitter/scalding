package com.twitter.scalding.spark_backend

object Iterators {

  /**
   * Partitions the iterator into runs of equivalent keys, then
   * returns an iterator of each distinct key followed by an
   * sub-iterator of its values.
   *
   * For example:
   *
   *   val lst = (1 to 8).map(i => (i / 3, i)).toList
   *   // List((0, 1), (0, 2), (1, 3), (1, 4), (1, 5), (2, 6), (2, 7), (2, 8))
   *
   *   groupSequential(lst.iterator).map(_.toList).toList
   *   // List((0, List(1, 2)), (1, List(3, 4, 5)), (2, List(6, 7, 8)))
   *
   * groupSequential() does not load the iterator's contents into
   * memory more than is absolutely necessary. This means it is safe
   * to use on very large (or potentially infinite) iterators as long
   * as the downstream consumer handles the results carefully.
   *
   * Note that the sub-iterators are fragile. That means that as soon
   * as you call .hasNext or .next on the top-level iterator, the
   * previous sub-iterator is invalidated (and will become empty). A
   * consequence of this is that you can only operate on one
   * sub-iterator at a time.
   *
   * Laws:
   *
   *   0. groupSequential(it).map(_._2.length).sum
   *      ~ it.length
   *
   *   1. groupSequential(it).flatMap { case (k, vs) => vs.map(v => (k, v)) }
   *      ~ it
   *
   *   2. given xs = lst.sortBy(_._1):
   *      groupSequential(xs.iterator).map { case (k, vs) => (k, vs.toList) }.toList
   *      ~ xs.groupBy(_._1).mapValues(_.map(_._2)).toList.sortBy(_._1)
   */
  def groupSequential[K, V](it: Iterator[(K, V)]): Iterator[(K, Iterator[V])] =
    // we need to look ahead to see if we have a key/value to start
    // grouping by. if not, this is quite easy!
    if (it.hasNext) {
      val (k0, v0) = it.next
      new GroupSequentialIterator(k0, v0, it)
    } else {
      Iterator.empty
    }

  /**
   * This is the internal class that powers Iterators.groupSequential.
   *
   * This process always requires one item worth of look-ahead, so
   * this class' constructor assumes the caller has already pulled
   * `(k0, v0)` from the front of `it`.
   */
  private class GroupSequentialIterator[K, V](k0: K, v0: V, it: Iterator[(K, V)])
    extends Iterator[(K, Iterator[V])] { parent =>

    var ready: InnerIterator = new InnerIterator(k0, v0, it)
    var child: InnerIterator = null

    @SuppressWarnings(Array("org.wartremover.warts.Return"))
    def loadNextChild(): Unit =
      if (ready != null) {
        // if we already have our next child ready, do nothing.
        ()
      } else {
        // we need to look ahead to find our next child.
        if (child != null) {
          // we had a currently-active child, so we need to shut that
          // down, and then start scanning to find a new key/value
          // pair. once we do, we should get a new child iterator
          // ready.
          val oldKey = child.key
          child.running = false
          child = null
          while (it.hasNext) {
            val (k1, v1) = it.next
            if (k1 != oldKey) {
              return prepare(k1, v1)
            }
          }
        } else if (it.hasNext) {
          // we didn't have a child iterator in progress, so just find
          // the next key/value and use those to prepare a child
          // iterator.
          val (k1, v1) = it.next
          return prepare(k1, v1)
        }
        // if we get here, it means we ran out of items and have
        // nothing to prepare.
        ready = null
      }

    def prepare(k1: K, v1: V): Unit =
      // initialize a new inner child iterator, using the given
      // key/value we already saw.
      ready = new InnerIterator(k1, v1, it)

    def hasNext: Boolean = {
      // prepare the next child (if necessary/possible), then see if
      // we have anything prepared or not.
      loadNextChild()
      ready != null
    }

    def next(): (K, Iterator[V]) = {
      // prepare the next child (if necessary/possible), then either
      // return it (if we were successful) or throw an error.
      loadNextChild()
      child = ready
      ready = null

      if (child == null) {
        throw new NoSuchElementException("next on empty iterator")
      } else {
        (child.key, child)
      }
    }

    class InnerIterator(val key: K, v0: V, it: Iterator[(K, V)]) extends Iterator[V] {
      var running: Boolean = true
      var saved: V = v0

      // since we're always looking one item ahead, we should already
      // know if we have something else to return or not.
      def hasNext: Boolean =
        running

      def next(): V =
        if (running) {
          // if we're running, we definitely have something saved that we can return.
          val res = saved
          if (it.hasNext) {
            // we have another item, so either it matches our key, and
            // we should save it for later, or else it's a new key, and
            // we should tell our parent to prepare the next inner
            // iterator.
            val (k1, v1) = it.next
            if (k1 == key) {
              saved = v1
            } else {
              running = false
              parent.child = null
              parent.prepare(k1, v1)
            }
          } else {
            // no more items means we're not running after this return.
            running = false
          }
          res
        } else {
          // we said we were done so now throw!
          throw new NoSuchElementException("next on empty iterator")
        }
    }
  }
}
