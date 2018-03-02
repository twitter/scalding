package com.twitter.scalding.spark_backend

import com.stripe.dagon.{ FunctionK, Memoize }
import com.twitter.scalding.typed._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

object SparkPlanner {

  private def widen[A, B >: A](rdd: RDD[A]): RDD[B] = rdd.asInstanceOf[RDD[B]]

  private def cast[A](rdd: RDD[_]): RDD[A] = rdd.asInstanceOf[RDD[A]]

  // TODO, this may be just inefficient, or it may be wrong
  implicit private def fakeClassTag[A]: ClassTag[A] = ClassTag(classOf[AnyRef]).asInstanceOf[ClassTag[A]]

  /**
   * Convert a TypedPipe to an RDD
   */
  def plan(ctx: SparkContext)(implicit ec: ExecutionContext): FunctionK[TypedPipe, RDD] =
    Memoize.functionK(new Memoize.RecursiveK[TypedPipe, RDD] {
      import TypedPipe._

      def toFunction[A] = {
        case (cp @ CounterPipe(_), rec) =>
          // TODO: counters not yet supported
          def go[A](p: CounterPipe[A]): RDD[A] = {
            rec(p.pipe).map(_._1)
          }
          widen(go(cp))
        case (cp @ CrossPipe(_, _), rec) =>
          def go[A, B](cp: CrossPipe[A, B]): RDD[(A, B)] =
            rec(cp.viaHashJoin)
          cast(go(cp))
        case (CrossValue(left, EmptyValue), rec) => widen(rec(EmptyTypedPipe))
        case (CrossValue(left, LiteralValue(v)), rec) =>
          val op = rec(left) // linter:disable:UndesirableTypeInference
          op.map((_, v))
        case (CrossValue(left, ComputedValue(right)), rec) =>
          rec(CrossPipe(left, right))
        case (p: DebugPipe[a], rec) =>
          // There is really little that can be done here but println
          cast(rec[a](p.input.map { t: a => println(t); t }))
        case (EmptyTypedPipe, rec) =>
          ctx.emptyRDD[A]
        case (fk @ FilterKeys(_, _), rec) =>
          def go[K, V](node: FilterKeys[K, V]): RDD[(K, V)] = {
            val FilterKeys(pipe, fn) = node
            rec(pipe).filter { case (k, _) => fn(k) }
          }
          cast(go(fk))
        case (f @ Filter(_, _), rec) =>
          def go[T](f: Filter[T]): RDD[T] = {
            val Filter(p, fn) = f
            rec[T](p).filter(fn)
          }
          widen(go(f))
        case (f @ FlatMapValues(_, _), rec) =>
          def go[K, V, U](node: FlatMapValues[K, V, U]) = {
            val fn = node.fn
            rec(node.input).flatMap { case (k, v) => fn(v).map((k, _)) }
          }

          cast(go(f))
        case (FlatMapped(prev, fn), rec) =>
          val op = rec(prev) // linter:disable:UndesirableTypeInference
          op.flatMap(fn)
        case (ForceToDisk(pipe), rec) =>
          rec(pipe).persist(StorageLevel.DISK_ONLY)
        case (Fork(pipe), rec) =>
          rec(pipe).persist(StorageLevel.MEMORY_ONLY)
        case (IterablePipe(iterable), _) =>
          ctx.makeRDD(iterable.toSeq, 1)
        case (f @ MapValues(_, _), rec) =>
          def go[K, V, U](node: MapValues[K, V, U]) = {
            val mvfn = node.fn
            val op = rec(node.input)
            op.map { case (k, v) => (k, mvfn(v)) }
          }
          cast(go(f))
        case (Mapped(input, fn), rec) =>
          val op = rec(input) // linter:disable:UndesirableTypeInference
          op.map(fn)
        case (m @ MergedTypedPipe(_, _), rec) =>
          def go[A](m: MergedTypedPipe[A]): RDD[A] = {
            widen(rec(m.left)) ++ widen(rec(m.left))
          }
          widen(go(m))
        case (SourcePipe(src), rec) =>
          // TODO
          ???
        case (slk @ SumByLocalKeys(_, _), rec) =>
          def sum[K, V](sblk: SumByLocalKeys[K, V]): RDD[(K, V)] = {
            val SumByLocalKeys(p, sg) = sblk
            ???
          }
          cast(sum(slk))

        case (tp: TrappedPipe[a], rec) => cast(rec[a](tp.input))
        // this can be interpretted as catching any exception
        // on the map-phase until the next partition, so it can
        // be made to work by changing Op to return all
        // the values that fail on error

        case (wd: WithDescriptionTypedPipe[a], rec) =>
          // TODO we could optionally print out the descriptions
          // after the future completes
          cast(rec[a](wd.input))

        case (woc: WithOnComplete[a], rec) =>
          // TODO
          cast(rec[a](woc.input))

        case (hcg @ HashCoGroup(_, _, _), rec) =>
          def go[K, V1, V2, R](hcg: HashCoGroup[K, V1, V2, R]): RDD[(K, R)] = {
            ???
          }
          cast(go(hcg))

        case (CoGroupedPipe(cg), rec) =>
          def go[K, V](cg: CoGrouped[K, V]): RDD[(K, V)] = {
            val inputs = cg.inputs
            val joinf = cg.joinFunction
            //Op.BulkJoin(inputs.map(rec(_)), joinf)
            ???
          }
          cast(go(cg))

        case (ReduceStepPipe(ir @ IdentityReduce(_, _, _, descriptions, _)), rec) =>
          def go[K, V1, V2](ir: IdentityReduce[K, V1, V2]): RDD[(K, V2)] = {
            type OpT[V] = RDD[(K, V)]
            val op = rec(ir.mapped)
            ir.evidence.subst[OpT](op)
          }
          cast(go(ir))
        case (ReduceStepPipe(uir @ UnsortedIdentityReduce(_, _, _, descriptions, _)), rec) =>
          def go[K, V1, V2](uir: UnsortedIdentityReduce[K, V1, V2]): RDD[(K, V2)] = {
            type OpT[V] = RDD[(K, V)]
            val op = rec(uir.mapped)
            uir.evidence.subst[OpT](op)
          }
          cast(go(uir))
        case (ReduceStepPipe(IdentityValueSortedReduce(_, pipe, ord, _, _, _)), rec) =>
          def go[K, V](p: TypedPipe[(K, V)], ord: Ordering[V]) = {
            val op = rec(p)
            //Op.Reduce[K, V, V](op, { (k, vs) => vs }, Some(ord))
            ???
          }
          widen(go(pipe, ord))
        case (ReduceStepPipe(ValueSortedReduce(_, pipe, ord, fn, _, _)), rec) =>
          val op = rec(pipe)
          ???
        case (ReduceStepPipe(IteratorMappedReduce(_, pipe, fn, _, _)), rec) =>
          val op = rec(pipe)
          ???
      }
    })
}
