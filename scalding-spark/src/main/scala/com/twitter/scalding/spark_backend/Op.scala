package com.twitter.scalding.spark_backend

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import com.twitter.scalding.{Config, FutureCache}
import com.twitter.scalding.typed.TypedSource

sealed abstract class Op[+A] {
  import Op.{Transformed, fakeClassTag}

  def run(session: SparkSession)(implicit ec: ExecutionContext): Future[RDD[_ <: A]]

  def map[B](fn: A => B): Op[B] =
    Transformed[A, B](this, _.map(fn))
  def concatMap[B](fn: A => TraversableOnce[B]): Op[B] =
    Transformed[A, B](this, _.flatMap(fn))
  def filter(fn: A => Boolean): Op[A] =
    Transformed[A, A](this, _.filter(fn))
  def persist(sl: StorageLevel): Op[A] =
    Transformed[A, A](this, _.persist(sl))
  def mapPartitions[B](fn: Iterator[A] => Iterator[B]): Op[B] =
    Transformed[A, B](this, _.mapPartitions(fn, preservesPartitioning = true))
}

object Op extends Serializable {
  // TODO, this may be just inefficient, or it may be wrong
  implicit private def fakeClassTag[A]: ClassTag[A] = ClassTag(classOf[AnyRef]).asInstanceOf[ClassTag[A]]

  implicit class PairOp[K, V](val op: Op[(K, V)]) extends AnyVal {
    def flatMapValues[U](fn: V => TraversableOnce[U]): Op[(K, U)] =
      Transformed[(K, V), (K, U)](op, _.flatMapValues(fn))
    def mapValues[U](fn: V => U): Op[(K, U)] =
      Transformed[(K, V), (K, U)](op, _.mapValues(fn))
    def mapGroup[U](fn: (K, Iterator[V]) => Iterator[U])(implicit ordK: Ordering[K]): Op[(K, U)] =
      Transformed[(K, V), (K, U)](op, { rdd: RDD[(K, V)] =>
        val numPartitions = rdd.getNumPartitions
        val partitioner = rdd.partitioner.getOrElse(new HashPartitioner(numPartitions))
        val partitioned = rdd.repartitionAndSortWithinPartitions(partitioner)
        partitioned.mapPartitions({ its =>
          // since we are sorted, the adjacent keys are next to each other
          val grouped = Iterators.groupSequential(its)
          grouped.flatMap { case (k, vs) => fn(k, vs).map((k, _)) }
        }, preservesPartitioning = true)
      })

    def hashJoin[U, W](right: Op[(K, U)])(fn: (K, V, Iterable[U]) => Iterator[W]): Op[(K, W)] =
      HashJoinOp(op, right, fn)

    def sorted(implicit ordK: Ordering[K], ordV: Ordering[V]): Op[(K, V)] =
      Transformed[(K, V), (K, V)](op, { rdd: RDD[(K, V)] =>
        // The idea here is that we put the key and the value in
        // logical key, but partition only on the left part of the key
        val numPartitions = rdd.getNumPartitions
        val partitioner = rdd.partitioner.getOrElse(new HashPartitioner(numPartitions))
        val keyOnlyPartioner = KeyHashPartitioner(partitioner)
        val unitValue: RDD[((K, V), Unit)] = rdd.map { kv => (kv, ()) }
        val partitioned = unitValue.repartitionAndSortWithinPartitions(keyOnlyPartioner)
        partitioned.mapPartitions({ its =>
          // discard the unit value
          its.map { case (kv, _) => kv }
        }, preservesPartitioning = true) // the keys haven't changed
      })

    def sortedMapGroup[U](fn: (K, Iterator[V]) => Iterator[U])(implicit ordK: Ordering[K], ordV: Ordering[V]): Op[(K, U)] =
      Transformed[(K, V), (K, U)](op, { rdd: RDD[(K, V)] =>
        // The idea here is that we put the key and the value in
        // logical key, but partition only on the left part of the key
        val numPartitions = rdd.getNumPartitions
        val partitioner = rdd.partitioner.getOrElse(new HashPartitioner(numPartitions))
        val keyOnlyPartioner = KeyHashPartitioner(partitioner)
        val unitValue: RDD[((K, V), Unit)] = rdd.map { kv => (kv, ()) }
        val partitioned = unitValue.repartitionAndSortWithinPartitions(keyOnlyPartioner)
        partitioned.mapPartitions({ its =>
          // discard the unit value
          val kviter = its.map { case (kv, _) => kv }
          // since we are sorted first by key, then value, the keys are grouped
          val grouped = Iterators.groupSequential(kviter)
          grouped.flatMap { case (k, vs) => fn(k, vs).map((k, _)) }
        }, preservesPartitioning = true) // the keys haven't changed
      })
  }

  private case class KeyHashPartitioner(partitioner: Partitioner) extends Partitioner {
    override def numPartitions: Int = partitioner.numPartitions

    override def getPartition(keyValue: Any): Int = {
      val key: Any = keyValue.asInstanceOf[(Any, Any)]._1
      partitioner.getPartition(key)
    }

  }

  implicit class InvariantOp[A](val op: Op[A]) extends AnyVal {
    def ++(that: Op[A]): Op[A] =
      op match {
        case Empty => that
        case nonEmpty =>
          that match {
            case Empty => nonEmpty
            case thatNE =>
              Merged(nonEmpty, thatNE)
          }
      }
  }

  object Empty extends Op[Nothing] {
    def run(session: SparkSession)(implicit ec: ExecutionContext) =
      Future(session.sparkContext.emptyRDD[Nothing])

    override def map[B](fn: Nothing => B): Op[B] = this
    override def concatMap[B](fn: Nothing => TraversableOnce[B]): Op[B] = this
    override def filter(fn: Nothing => Boolean): Op[Nothing] = this
    override def persist(sl: StorageLevel): Op[Nothing] = this
    override def mapPartitions[B](fn: Iterator[Nothing] => Iterator[B]): Op[B] = this
  }

  final case class FromIterable[A](iterable: Iterable[A]) extends Op[A] {
    def run(session: SparkSession)(implicit ec: ExecutionContext): Future[RDD[_ <: A]] =
      Future(session.sparkContext.makeRDD(iterable.toSeq, 1))
  }

  final case class Source[A](conf: Config, original: TypedSource[A], input: Option[SparkSource[A]]) extends Op[A] {
    def run(session: SparkSession)(implicit ec: ExecutionContext): Future[RDD[_ <: A]] =
      input match {
        case None => Future.failed(new IllegalArgumentException(s"source $original was not connected to a spark source"))
        case Some(src) => src.read(session, conf)
      }
  }

  private def widen[A](r: RDD[_ <: A]): RDD[A] =
    //r.map { a => a }
    // or we could just cast
    r.asInstanceOf[RDD[A]]

  final case class Transformed[Z, A](input: Op[Z], fn: RDD[Z] => RDD[A]) extends Op[A] {
    @transient private val cache = new FutureCache[SparkSession, RDD[_ <: A]]

    def run(session: SparkSession)(implicit ec: ExecutionContext): Future[RDD[_ <: A]] =
      cache.getOrElseUpdate(session,
        input.run(session).map { rdd => fn(widen(rdd)) })
  }

  final case class Merged[A](left: Op[A], right: Op[A]) extends Op[A] {
    @transient private val cache = new FutureCache[SparkSession, RDD[_ <: A]]

    def run(session: SparkSession)(implicit ec: ExecutionContext): Future[RDD[_ <: A]] =
      cache.getOrElseUpdate(session, {
        // start running in parallel
        val lrdd = left.run(session)
        val rrdd = right.run(session)
        for {
          l <- lrdd
          r <- rrdd
        } yield widen[A](l) ++ widen[A](r)
      })
  }

  final case class HashJoinOp[A, B, C, D](left: Op[(A, B)], right: Op[(A, C)], joiner: (A, B, Iterable[C]) => Iterator[D]) extends Op[(A, D)] {
    @transient private val cache = new FutureCache[SparkSession, RDD[_ <: (A, D)]]

    def run(session: SparkSession)(implicit ec: ExecutionContext): Future[RDD[_ <: (A, D)]] =
      cache.getOrElseUpdate(session, {
        // start running in parallel
        val rrdd = right.run(session)
        val lrdd = left.run(session)

        rrdd.flatMap { rightRdd =>
          // TODO: spark has some thing to send replicated data to nodes
          // we should materialize the small side, use the above, then
          // implement a join using mapPartitions
          val rightMap: Map[A, List[C]] = rightRdd
            .toLocalIterator
            .toList
            .groupBy(_._1)
            .map { case (k, vs) => (k, vs.map(_._2)) }

          val bcastMap = session.sparkContext.broadcast(rightMap)
          lrdd.map { leftrdd =>

            val localJoiner = joiner

            leftrdd.mapPartitions({ it: Iterator[(A, B)] =>
              val rightMap = bcastMap.value
              it.flatMap { case (a, b) => localJoiner(a, b, rightMap.getOrElse(a, Nil)).map((a, _)) }
            }, preservesPartitioning = true)
          }
        }
      })

  }
}
