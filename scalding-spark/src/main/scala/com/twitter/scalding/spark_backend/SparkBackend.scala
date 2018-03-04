package com.twitter.scalding.spark_backend

import com.stripe.dagon.{ FunctionK, Memoize }
import com.twitter.scalding.Config
import com.twitter.scalding.typed._
import com.twitter.scalding.typed.functions.{ DebugFn, FilterKeysToFilter, SubTypes }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

object SparkPlanner {

  // TODO, this may be just inefficient, or it may be wrong
  implicit private def fakeClassTag[A]: ClassTag[A] = ClassTag(classOf[AnyRef]).asInstanceOf[ClassTag[A]]

  /**
   * This is a covariant wrapper for RDD
   */
  sealed abstract class R[+T] { self =>
    type A
    def evidence: SubTypes[A, T]
    def rdd: RDD[A]
    def map[B](fn: T => B): R[B] = {
      type F[-X] = X => B
      R(rdd.map(evidence.subst[F](fn)))
    }
    def concatMap[B](fn: T => TraversableOnce[B]): R[B] = {
      type F[-X] = X => TraversableOnce[B]
      R(rdd.flatMap(evidence.subst[F](fn)))
    }
    def filter(fn: T => Boolean): R[T] = {
      type F[-X] = X => Boolean
      new R[T] {
        type A = self.A
        val evidence = self.evidence
        val rdd = self.rdd.filter(self.evidence.subst[F](fn))
      }
    }

    def persist(sl: StorageLevel): R[T] =
      new R[T] {
        type A = self.A
        val evidence = self.evidence
        val rdd = self.rdd.persist(sl)
      }

    def mapPartitions[U](fn: Iterator[T] => Iterator[U]): R[U] = {
      type F[-X] = Iterator[X] => Iterator[U]
      R(rdd.mapPartitions(evidence.subst[F](fn), preservesPartitioning = true))
    }
  }

  object R {
    def apply[A1](rdd1: RDD[A1]): R[A1] =
      new R[A1] {
        type A = A1
        val evidence = SubTypes.fromSubType[A, A]
        val rdd = rdd1
      }

    def toRDD[A1](r: R[A1]): RDD[A1] =
      // this would be maybe slower, but safe:
      r.rdd.map(r.evidence.toEv)
    // or we can just cast
    //r.rdd.asInstanceOf[RDD[A1]]

    def empty(ctx: SparkContext): R[Nothing] =
      R(ctx.emptyRDD[Nothing])

    def fromIterable[A](ctx: SparkContext, iter: Iterable[A]): R[A] =
      R(ctx.makeRDD(iter.toSeq, 1))

    def merge[A1](left: R[A1], right: R[A1]): R[A1] =
      R(toRDD(left) ++ toRDD(right))

    implicit class PairR[K, V](val kv: R[(K, V)]) extends AnyVal {
      def flatMapValues[U](fn: V => TraversableOnce[U]): R[(K, U)] =
        R(toRDD(kv).flatMapValues(fn))
      def mapValues[U](fn: V => U): R[(K, U)] =
        R(toRDD(kv).mapValues(fn))
    }
  }

  /**
   * Convert a TypedPipe to an RDD
   */
  def plan(ctx: SparkContext, config: Config)(implicit ec: ExecutionContext): FunctionK[TypedPipe, R] =
    Memoize.functionK(new Memoize.RecursiveK[TypedPipe, R] {
      import TypedPipe._

      def toFunction[A] = {
        case (cp @ CounterPipe(_), rec) =>
          // TODO: counters not yet supported
          def go[A](p: CounterPipe[A]): R[A] = {
            rec(p.pipe).map(_._1)
          }
          go(cp)
        case (cp @ CrossPipe(_, _), rec) =>
          def go[A, B](cp: CrossPipe[A, B]): R[(A, B)] =
            rec(cp.viaHashJoin)
          go(cp)
        case (CrossValue(left, EmptyValue), rec) => rec(EmptyTypedPipe)
        case (CrossValue(left, LiteralValue(v)), rec) =>
          val op = rec(left) // linter:disable:UndesirableTypeInference
          op.map((_, v))
        case (CrossValue(left, ComputedValue(right)), rec) =>
          rec(CrossPipe(left, right))
        case (p: DebugPipe[a], rec) =>
          // There is really little that can be done here but println
          rec[a](p.input.map(DebugFn()))
        case (EmptyTypedPipe, rec) =>
          R.empty(ctx)
        case (fk @ FilterKeys(_, _), rec) =>
          def go[K, V](node: FilterKeys[K, V]): R[(K, V)] = {
            val FilterKeys(pipe, fn) = node
            rec(pipe).filter(FilterKeysToFilter(fn))
          }
          go(fk)
        case (f @ Filter(_, _), rec) =>
          def go[T](f: Filter[T]): R[T] = {
            val Filter(p, fn) = f
            rec[T](p).filter(fn)
          }
          go(f)
        case (f @ FlatMapValues(_, _), rec) =>
          def go[K, V, U](node: FlatMapValues[K, V, U]) = {
            val fn = node.fn
            rec(node.input).flatMapValues(fn)
          }

          go(f)
        case (FlatMapped(prev, fn), rec) =>
          val op = rec(prev) // linter:disable:UndesirableTypeInference
          op.concatMap(fn)
        case (ForceToDisk(pipe), rec) =>
          rec(pipe).persist(StorageLevel.DISK_ONLY)
        case (Fork(pipe), rec) =>
          rec(pipe).persist(StorageLevel.MEMORY_ONLY)
        case (IterablePipe(iterable), _) =>
          R.fromIterable(ctx, iterable)
        case (f @ MapValues(_, _), rec) =>
          def go[K, V, U](node: MapValues[K, V, U]): R[(K, U)] =
            rec(node.input).mapValues(node.fn)
          go(f)
        case (Mapped(input, fn), rec) =>
          val op = rec(input) // linter:disable:UndesirableTypeInference
          op.map(fn)
        case (m @ MergedTypedPipe(_, _), rec) =>
          def go[A](m: MergedTypedPipe[A]): R[A] = {
            R.merge(rec(m.left), rec(m.right))
          }
          go(m)
        case (SourcePipe(src), rec) =>
          // TODO
          ???
        case (slk @ SumByLocalKeys(_, _), rec) =>
          def sum[K, V](sblk: SumByLocalKeys[K, V]): R[(K, V)] = {
            // we can use Algebird's SummingCache https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/SummingCache.scala#L36
            // plus mapPartitions to implement this
            val SumByLocalKeys(p, sg) = sblk
            ???
          }
          sum(slk)

        case (tp: TrappedPipe[a], rec) => rec[a](tp.input)
        // this can be interpretted as catching any exception
        // on the map-phase until the next partition, so it can
        // be made to work by changing Op to return all
        // the values that fail on error

        case (wd: WithDescriptionTypedPipe[a], rec) =>
          // TODO we could optionally print out the descriptions
          // after the future completes
          rec[a](wd.input)

        case (woc: WithOnComplete[a], rec) =>
          // TODO
          rec[a](woc.input)

        case (hcg @ HashCoGroup(_, _, _), rec) =>
          def go[K, V1, V2, W](hcg: HashCoGroup[K, V1, V2, W]): R[(K, W)] = {
            ???
          }
          go(hcg)

        case (CoGroupedPipe(cg), rec) =>
          def go[K, V](cg: CoGrouped[K, V]): R[(K, V)] = {
            val inputs = cg.inputs
            val joinf = cg.joinFunction
            //Op.BulkJoin(inputs.map(rec(_)), joinf)
            ???
          }
          go(cg)

        case (ReduceStepPipe(ir @ IdentityReduce(_, _, _, descriptions, _)), rec) =>
          def go[K, V1, V2](ir: IdentityReduce[K, V1, V2]): R[(K, V2)] = {
            type OpT[V] = R[(K, V)]
            val op = rec(ir.mapped)
            ir.evidence.subst[OpT](op)
          }
          go(ir)
        case (ReduceStepPipe(uir @ UnsortedIdentityReduce(_, _, _, descriptions, _)), rec) =>
          def go[K, V1, V2](uir: UnsortedIdentityReduce[K, V1, V2]): R[(K, V2)] = {
            type OpT[V] = R[(K, V)]
            val op = rec(uir.mapped)
            uir.evidence.subst[OpT](op)
          }
          go(uir)
        case (ReduceStepPipe(IdentityValueSortedReduce(_, pipe, ord, _, _, _)), rec) =>
          def go[K, V](p: TypedPipe[(K, V)], ord: Ordering[V]) = {
            val op = rec(p)
            //Op.Reduce[K, V, V](op, { (k, vs) => vs }, Some(ord))
            ???
          }
          go(pipe, ord)
        case (ReduceStepPipe(ValueSortedReduce(_, pipe, ord, fn, _, _)), rec) =>
          val op = rec(pipe)
          ???
        case (ReduceStepPipe(IteratorMappedReduce(_, pipe, fn, _, _)), rec) =>
          val op = rec(pipe)
          ???
      }
    })
}
