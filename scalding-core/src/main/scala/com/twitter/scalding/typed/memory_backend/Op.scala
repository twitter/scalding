package com.twitter.scalding.typed.memory_backend

import com.twitter.scalding.typed._
import java.util.{ ArrayList, Collections }
import scala.collection.JavaConverters._
import scala.collection.mutable.{ ArrayBuffer, Map => MMap }
import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext, Promise }

sealed trait Op[+O] {
  def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[_ <: O]]

  def concatMap[O1](fn: O => TraversableOnce[O1]): Op[O1] =
    transform { in: IndexedSeq[O] =>
      val res = ArrayBuffer[O1]()
      val it = in.iterator
      while(it.hasNext) {
        val i = it.next
        fn(i).foreach(res += _)
      }
      res
    }

  def map[O1](fn: O => O1): Op[O1] =
    Op.MapOp(this, fn)

  def filter(fn: O => Boolean): Op[O] =
    Op.Filter(this, fn)

  def transform[O1 >: O, O2](fn: IndexedSeq[O1] => ArrayBuffer[O2]): Op[O2] =
    Op.Transform[O1, O2](this, fn)

  def materialize: Op[O] =
    Op.Materialize(this)
}
object Op {
  def source[I](i: Iterable[I]): Op[I] = Source(_ => Future.successful(i.iterator))
  def empty[I]: Op[I] = source(Nil)

  final case class Source[I](input: ConcurrentExecutionContext => Future[Iterator[I]]) extends Op[I] {

    def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[I]] =
      input(cec).map(ArrayBuffer.empty[I] ++= _)
  }

  // Here we need to make a copy on each result
  final case class Materialize[O](op: Op[O]) extends Op[O] {
    private[this] val promiseBox: AtomicBox[Option[Promise[ArrayBuffer[_ <: O]]]] = new AtomicBox(None)

    def result(implicit cec: ConcurrentExecutionContext) = {
      val either = promiseBox.update {
        case None =>
          val promise = Promise[ArrayBuffer[_ <: O]]()
          (Some(promise), Right(promise))
        case s@Some(promise) =>
          (s, Left(promise))
      }

      val fut = either match {
        case Right(promise) =>
          // This is the one case where we call the op
          promise.completeWith(op.result)
          promise.future
        case Left(promise) =>
          // we already started the previous work
          promise.future
      }
      fut.map(ArrayBuffer.concat(_))
    }
  }

  final case class Concat[O](left: Op[O], right: Op[O]) extends Op[O] {
    def result(implicit cec: ConcurrentExecutionContext) = {
      val f1 = left.result
      val f2 = right.result
      f1.zip(f2).map { case (l, r) =>
        if (l.size > r.size) l.asInstanceOf[ArrayBuffer[O]] ++= r
        else r.asInstanceOf[ArrayBuffer[O]] ++= l
      }
    }
  }

  // We reuse the input on map
  final case class MapOp[I, O](input: Op[I], fn: I => O) extends Op[O] {
    def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[O]] =
      input.result.map { array =>
        val res: ArrayBuffer[O] = array.asInstanceOf[ArrayBuffer[O]]
        var pos = 0
        while(pos < array.length) {
          res.update(pos, fn(array(pos)))
          pos = pos + 1
        }
        res
      }
  }
  // We reuse the input on filter
  final case class Filter[I](input: Op[I], fn: I => Boolean) extends Op[I] {
    def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[I]] =
      input.result.map { array0 =>
        val array = array0.asInstanceOf[ArrayBuffer[I]]
        var pos = 0
        var writePos = 0
        while(pos < array.length) {
          val item = array(pos)
          if (fn(item)) {
            array(writePos) = item
            writePos = writePos + 1
          }
          pos = pos + 1
        }
        // trim the tail off
        array.remove(writePos, array.length - writePos)
        array
      }
  }

  final case class OnComplete[O](of: Op[O], fn: () => Unit) extends Op[O] {
    def result(implicit cec: ConcurrentExecutionContext) = {
      val res = of.result
      res.onComplete(_ => fn())
      res
    }
  }

  final case class Transform[I, O](input: Op[I], fn: IndexedSeq[I] => ArrayBuffer[O]) extends Op[O] {
    def result(implicit cec: ConcurrentExecutionContext) =
      input.result.map(fn)
  }

  final case class Reduce[K, V1, V2](
    input: Op[(K, V1)],
    fn: (K, Iterator[V1]) => Iterator[V2],
    ord: Option[Ordering[V1]]
    ) extends Op[(K, V2)] {

    def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[(K, V2)]] =
      input.result.map { kvs =>
        val valuesByKey = MMap[K, ArrayList[V1]]()
        def add(kv: (K, V1)): Unit = {
          val vs = valuesByKey.getOrElseUpdate(kv._1, new ArrayList[V1]())
          vs.add(kv._2)
        }
        kvs.foreach(add)

        /*
         * This portion could be parallelized for each key, or we could split
         * the keys into as many groups as there are CPUs and process that way
         */
        val res = ArrayBuffer[(K, V2)]()
        valuesByKey.foreach { case (k, vs) =>
          ord.foreach(Collections.sort[V1](vs, _))
          val v2iter = fn(k, vs.iterator.asScala)
          while(v2iter.hasNext) {
            res += ((k, v2iter.next))
          }
        }
        res
    }
  }

  final case class Join[A, B, C](
    opA: Op[A],
    opB: Op[B],
    fn: (IndexedSeq[A], IndexedSeq[B]) => ArrayBuffer[C]) extends Op[C] {

    def result(implicit cec: ConcurrentExecutionContext) = {
      // start both futures in parallel
      val f1 = opA.result
      val f2 = opB.result
      f1.zip(f2).map { case (a, b) => fn(a, b) }
    }
  }

  final case class BulkJoin[K, A](ops: List[Op[(K, Any)]], joinF: MultiJoinFunction[K, A]) extends Op[(K, A)] {
    def result(implicit cec: ConcurrentExecutionContext) =
      Future.traverse(ops)(_.result)
        .map { items =>
          // TODO this is not by any means optimal.
          // we could copy into arrays then sort by key and iterate
          // each through in K sorted order
          val maps: List[Map[K, Iterable[(K, Any)]]] = items.map { kvs =>
            val kvMap: Map[K, Iterable[(K, Any)]] = kvs.groupBy(_._1)
            kvMap
          }

          val allKeys = maps.iterator.flatMap(_.keys.iterator).toSet
          val result = ArrayBuffer[(K, A)]()
          allKeys.foreach { k =>
            maps.map(_.getOrElse(k, Nil)) match {
              case h :: tail =>
                joinF(k, h.iterator.map(_._2), tail.map(_.map(_._2))).foreach { a =>
                  result += ((k, a))
                }
              case other => sys.error(s"unreachable: $other, $k")
            }
          }

          result
        }
  }
}
