package com.twitter.scalding.typed.memory_backend

import scala.collection.mutable.ArrayBuffer
import com.stripe.dagon.{ Memoize, FunctionK }
import com.twitter.scalding.typed._
import com.twitter.scalding.Config

object MemoryPlanner {
  /**
   * This builds an new memoizing planner
   * that reads from the given MemoryMode.
   *
   * Note, this assumes all forks are made explicit
   * in the graph, so it is up to any caller
   * to make sure that optimization rule has first
   * been applied
   */
  def planner(conf: Config, srcs: Resolver[TypedSource, MemorySource]): FunctionK[TypedPipe, Op] =
    Memoize.functionK(new Memoize.RecursiveK[TypedPipe, Op] {
      import TypedPipe._

      def toFunction[T] = {
        case (CounterPipe(pipe), rec) =>
          // TODO: counters not yet supported, but can be with an concurrent hashmap
          rec(pipe.map(_._1))
        case (cp @ CrossPipe(_, _), rec) =>
          rec(cp.viaHashJoin)
        case (CrossValue(left, EmptyValue), _) => Op.empty
        case (CrossValue(left, LiteralValue(v)), rec) =>
          val op = rec(left) // linter:disable:UndesirableTypeInference
          op.map((_, v))
        case (CrossValue(left, ComputedValue(right)), rec) =>
          rec(CrossPipe(left, right))
        case (DebugPipe(p), rec) =>
          // There is really little that can be done here but println
          rec(p.map { t => println(t); t })

        case (EmptyTypedPipe, _) =>
          // just use an empty iterable pipe.
          Op.empty[T]

        case (fk @ FilterKeys(_, _), rec) =>
          def go[K, V](node: FilterKeys[K, V]): Op[(K, V)] = {
            val FilterKeys(pipe, fn) = node
            rec(pipe).concatMap { case (k, v) => if (fn(k)) { (k, v) :: Nil } else Nil }
          }
          go(fk)

        case (f @ Filter(_, _), rec) =>
          def go[T](f: Filter[T]): Op[T] = {
            val Filter(p, fn) = f
            rec(p).filter(fn)
          }
          go(f)

        case (f @ FlatMapValues(_, _), rec) =>
          def go[K, V, U](node: FlatMapValues[K, V, U]) = {
            val fn = node.fn
            rec(node.input).concatMap { case (k, v) => fn(v).map((k, _)) }
          }

          go(f)

        case (FlatMapped(prev, fn), rec) =>
          rec(prev).concatMap(fn) // linter:disable:UndesirableTypeInference

        case (ForceToDisk(pipe), rec) =>
          rec(pipe).materialize

        case (Fork(pipe), rec) =>
          rec(pipe).materialize

        case (IterablePipe(iterable), _) =>
          Op.source(iterable)

        case (f @ MapValues(_, _), rec) =>
          def go[K, V, U](node: MapValues[K, V, U]) = {
            val mvfn = node.fn
            rec(node.input).map { case (k, v) => (k, mvfn(v)) }
          }

          go(f)

        case (Mapped(input, fn), rec) =>
          rec(input).map(fn) // linter:disable:UndesirableTypeInference

        case (MergedTypedPipe(left, right), rec) =>
          Op.Concat(rec(left), rec(right))

        case (SourcePipe(src), _) =>
          val optsrc = srcs(src)
          Op.Source({ cec => MemorySource.readOption(optsrc, src.toString)(cec) })

        case (slk @ SumByLocalKeys(_, _), rec) =>
          def sum[K, V](sblk: SumByLocalKeys[K, V]) = {
            val SumByLocalKeys(p, sg) = sblk

            rec(p).transform[(K, V), (K, V)] { kvs =>
              val map = collection.mutable.Map.empty[K, V]
              val iter = kvs.iterator
              while (iter.hasNext) {
                val (k, v) = iter.next
                map(k) = map.get(k) match {
                  case None => v
                  case Some(v1) => sg.plus(v1, v)
                }
              }
              val res = new ArrayBuffer[(K, V)](map.size)
              map.foreach { res += _ }
              res
            }
          }
          sum(slk)

        case (TrappedPipe(input, _, _), rec) =>
          // this can be interpretted as catching any exception
          // on the map-phase until the next partition, so it can
          // be made to work by changing Op to return all
          // the values that fail on error
          rec(input)

        case (WithDescriptionTypedPipe(pipe, descriptions), rec) =>
          // TODO we could optionally print out the descriptions
          // after the future completes
          rec(pipe)

        case (WithOnComplete(pipe, fn), rec) =>
          Op.OnComplete(rec(pipe), fn)

        case (hcg @ HashCoGroup(_, _, _), rec) =>
          def go[K, V1, V2, R](hcg: HashCoGroup[K, V1, V2, R]) = {
            val leftOp = rec(hcg.left)
            val rightOp = rec(ReduceStepPipe(HashJoinable.toReduceStep(hcg.right)))
            Op.Join[(K, V1), (K, V2), (K, R)](leftOp, rightOp, { (v1s, v2s) =>
              val kv2 = v2s.groupBy(_._1)
              val result = new ArrayBuffer[(K, R)]()
              v1s.foreach {
                case (k, v1) =>
                  val v2 = kv2.getOrElse(k, Nil).map(_._2)
                  result ++= hcg.joiner(k, v1, v2).map((k, _))
              }
              result
            })
          }
          go(hcg)

        case (CoGroupedPipe(cg), rec) =>
          def go[K, V](cg: CoGrouped[K, V]) = {
            Op.BulkJoin(cg.inputs.map(rec(_)), cg.joinFunction)
          }
          go(cg)

        case (ReduceStepPipe(ir @ IdentityReduce(_, _, _, descriptions, _)), rec) =>
          def go[K, V1, V2](ir: IdentityReduce[K, V1, V2]): Op[(K, V2)] = {
            type OpT[V] = Op[(K, V)]
            val op = rec(ir.mapped)
            ir.evidence.subst[OpT](op)
          }
          go(ir)
        case (ReduceStepPipe(uir @ UnsortedIdentityReduce(_, _, _, descriptions, _)), rec) =>
          def go[K, V1, V2](uir: UnsortedIdentityReduce[K, V1, V2]): Op[(K, V2)] = {
            type OpT[V] = Op[(K, V)]
            val op = rec(uir.mapped)
            uir.evidence.subst[OpT](op)
          }
          go(uir)
        case (ReduceStepPipe(IdentityValueSortedReduce(_, pipe, ord, _, _, _)), rec) =>
          def go[K, V](p: TypedPipe[(K, V)], ord: Ordering[V]) = {
            val op = rec(p)
            Op.Reduce[K, V, V](op, { (k, vs) => vs }, Some(ord))
          }
          go(pipe, ord)
        case (ReduceStepPipe(ValueSortedReduce(_, pipe, ord, fn, _, _)), rec) =>
          Op.Reduce(rec(pipe), fn, Some(ord))
        case (ReduceStepPipe(IteratorMappedReduce(_, pipe, fn, _, _)), rec) =>
          Op.Reduce(rec(pipe), fn, None)
      }
    })

}

