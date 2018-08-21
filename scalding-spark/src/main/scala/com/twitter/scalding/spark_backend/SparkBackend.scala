package com.twitter.scalding.spark_backend

import com.stripe.dagon.{ FunctionK, Memoize }
import com.twitter.algebird.Semigroup
import com.twitter.scalding.Config
import com.twitter.scalding.typed._
import com.twitter.scalding.typed.functions.{ DebugFn, FilterKeysToFilter }
import java.util.{ LinkedHashMap => JLinkedHashMap, Map => JMap }
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.{ Map => MMap, ArrayBuffer }

object SparkPlanner {
  /**
   * Convert a TypedPipe to an RDD
   */
  def plan(config: Config, srcs: Resolver[TypedSource, SparkSource]): FunctionK[TypedPipe, Op] =
    Memoize.functionK(new Memoize.RecursiveK[TypedPipe, Op] {
      import TypedPipe._

      def toFunction[A] = {
        case (cp @ CounterPipe(_), rec) =>
          // TODO: counters not yet supported
          def go[A](p: CounterPipe[A]): Op[A] = {
            rec(p.pipe).map(_._1)
          }
          go(cp)
        case (cp @ CrossPipe(_, _), rec) =>
          def go[A, B](cp: CrossPipe[A, B]): Op[(A, B)] =
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
          Op.Empty
        case (fk @ FilterKeys(_, _), rec) =>
          def go[K, V](node: FilterKeys[K, V]): Op[(K, V)] = {
            val FilterKeys(pipe, fn) = node
            rec(pipe).filter(FilterKeysToFilter(fn))
          }
          go(fk)
        case (f @ Filter(_, _), rec) =>
          def go[T](f: Filter[T]): Op[T] = {
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
          Op.FromIterable(iterable)
        case (f @ MapValues(_, _), rec) =>
          def go[K, V, U](node: MapValues[K, V, U]): Op[(K, U)] =
            rec(node.input).mapValues(node.fn)
          go(f)
        case (Mapped(input, fn), rec) =>
          val op = rec(input) // linter:disable:UndesirableTypeInference
          op.map(fn)
        case (m @ MergedTypedPipe(_, _), rec) =>
          def go[A](m: MergedTypedPipe[A]): Op[A] =
            rec(m.left) ++ rec(m.right)
          go(m)
        case (SourcePipe(src), _) =>
          Op.Source(config, src, srcs(src))
        case (slk @ SumByLocalKeys(_, _), rec) =>
          def sum[K, V](sblk: SumByLocalKeys[K, V]): Op[(K, V)] = {
            // we can use Algebird's SummingCache https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/SummingCache.scala#L36
            // plus mapPartitions to implement this
            val SumByLocalKeys(p, sg) = sblk
            // TODO set a default in a better place
            val defaultCapacity = 10000
            val capacity = config.getMapSideAggregationThreshold.getOrElse(defaultCapacity)
            rec(p).mapPartitions(CachingSum(capacity, sg))
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
          def go[K, V1, V2, W](hcg: HashCoGroup[K, V1, V2, W]): Op[(K, W)] = {
            val leftOp = rec(hcg.left)
            val rightOp = rec(ReduceStepPipe(HashJoinable.toReduceStep(hcg.right)))
            leftOp.hashJoin(rightOp)(hcg.joiner)
          }
          go(hcg)

        case (CoGroupedPipe(cg), rec) =>
          planCoGroup(config, cg, rec)

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
        case (ReduceStepPipe(ivsr @ IdentityValueSortedReduce(_, _, _, _, _, _)), rec) =>
          def go[K, V1, V2](uir: IdentityValueSortedReduce[K, V1, V2]): Op[(K, V2)] = {
            type OpT[V] = Op[(K, V)]
            val op = rec(uir.mapped)
            val sortedOp = op.sorted(uir.keyOrdering, uir.valueSort)
            uir.evidence.subst[OpT](sortedOp)
          }
          go(ivsr)
        case (ReduceStepPipe(ValueSortedReduce(ordK, pipe, ordV, fn, _, _)), rec) =>
          val op = rec(pipe)
          op.sortedMapGroup(fn)(ordK, ordV)
        case (ReduceStepPipe(IteratorMappedReduce(ordK, pipe, fn, _, _)), rec) =>
          val op = rec(pipe)
          op.mapGroup(fn)(ordK)
      }
    })

  case class OnEmptyIterator[A](it: Iterator[A], fn: () => Unit) extends Iterator[A] {
    var fnMut: () => Unit = fn
    def hasNext = it.hasNext || {
      if (fnMut != null) { fnMut(); fnMut = null }
      false
    }

    def next = it.next
  }

  case class CachingSum[K, V](capacity: Int, semigroup: Semigroup[V]) extends Function1[Iterator[(K, V)], Iterator[(K, V)]] {
    def newCache(evicted: MMap[K, V]): JMap[K, V] = new JLinkedHashMap[K, V](capacity + 1, 0.75f, true) {
      override protected def removeEldestEntry(eldest: JMap.Entry[K, V]) =
        if (super.size > capacity) {
          evicted.put(eldest.getKey, eldest.getValue)
          true
        } else {
          false
        }
    }

    def apply(kvs: Iterator[(K, V)]) = {
      val evicted = MMap.empty[K, V]
      val currentCache = newCache(evicted)
      new Iterator[(K, V)] {
        var resultIterator: Iterator[(K, V)] = Iterator.empty

        def hasNext = kvs.hasNext || resultIterator.hasNext
        @annotation.tailrec
        def next: (K, V) = {
          if (resultIterator.hasNext) {
            resultIterator.next
          } else if (kvs.hasNext) {
            val (k, deltav) = kvs.next
            val vold = currentCache.get(k)
            if (vold == null) {
              currentCache.put(k, deltav)
            } else {
              currentCache.put(k, semigroup.plus(vold, deltav))
            }
            // let's see if we have anything to evict
            if (evicted.nonEmpty) {
              resultIterator = OnEmptyIterator(evicted.iterator, () => evicted.clear())
            }
            next
          } else if (currentCache.isEmpty) {
            throw new java.util.NoSuchElementException("next called on empty CachingSum Iterator")
          } else {
            // time to flush the cache
            import scala.collection.JavaConverters._
            val cacheIter = currentCache.entrySet.iterator.asScala.map { e => (e.getKey, e.getValue) }
            resultIterator = OnEmptyIterator(cacheIter, () => currentCache.clear())
            next
          }
        }
      }
    }
  }

  private def planHashJoinable[K, V](hj: HashJoinable[K, V], rec: FunctionK[TypedPipe, Op]): Op[(K, V)] =
    rec(TypedPipe.ReduceStepPipe(HashJoinable.toReduceStep(hj)))

  private def planCoGroup[K, V](config: Config, cg: CoGrouped[K, V], rec: FunctionK[TypedPipe, Op]): Op[(K, V)] = {
    import CoGrouped._

    cg match {
      case FilterKeys(cg, fn) =>
        planCoGroup(config, cg, rec).filter { case (k, _) => fn(k) }
      case MapGroup(cg, fn) =>
        // don't need to repartition, just mapPartitions
        // we know this because cg MUST be a join, there is no non-joined CoGrouped
        // so we know the output op planCoGroup(config, cg, rec) is already partitioned
        planCoGroup(config, cg, rec)
          .mapPartitions { its =>
            val grouped = Iterators.groupSequential(its)
            grouped.flatMap { case (k, vs) => fn(k, vs).map((k, _)) }
          }
      case pair @ Pair(_, _, _) =>
        // ideally, we do one partitioning of all the data,
        // but for now just do the naive thing:
        def planSide[A, B](cg: CoGroupable[A, B]): Op[(A, B)] =
          cg match {
            case hg: HashJoinable[A, B] => planHashJoinable(hg, rec)
            case cg: CoGrouped[A, B] => planCoGroup(config, cg, rec)
          }

        def planPair[A, B, C, D](p: Pair[A, B, C, D]): Op[(A, D)] = {
          val eleft: Op[(A, Either[B, C])] = planSide(p.larger).map { case (k, v) => (k, Left(v)) }
          val eright: Op[(A, Either[B, C])] = planSide(p.smaller).map { case (k, v) => (k, Right(v)) }
          val joinFn = p.fn
          (eleft ++ eright).sorted(p.keyOrdering, JoinOrdering()).mapPartitions { it =>
            val grouped = Iterators.groupSequential(it)
            grouped.flatMap {
              case (k, eithers) =>
                val kfn: Function2[Iterator[B], Iterable[C], Iterator[D]] = joinFn(k, _, _)
                JoinIterator[B, C, D](kfn)(eithers).map((k, _))
            }
          }
        }
        planPair(pair)

      case WithDescription(cg, d) =>
        // TODO handle descriptions in some way
        planCoGroup(config, cg, rec)
      case WithReducers(cg, r) =>
        // TODO handle the number of reducers, maybe by setting the number of partitions
        planCoGroup(config, cg, rec)
    }
  }

  // Put the rights first
  case class JoinOrdering[A, B]() extends Ordering[Either[A, B]] {
    def compare(left: Either[A, B], right: Either[A, B]): Int =
      if (left.isLeft && right.isRight) 1
      else if (left.isRight && right.isLeft) -1
      else 0
  }

  case class JoinIterator[A, B, C](fn: (Iterator[A], Iterable[B]) => Iterator[C]) extends Function1[Iterator[Either[A, B]], Iterator[C]] {
    @SuppressWarnings(Array("org.wartremover.warts.EitherProjectionPartial"))
    def apply(eitherIter: Iterator[Either[A, B]]) = {
      val buffered = eitherIter.buffered
      val bs: Iterable[B] = {
        @annotation.tailrec
        def loop(buf: ArrayBuffer[B]): Iterable[B] = {
          if (buffered.isEmpty) buf
          else if (buffered.head.isLeft) buf
          else {
            buf += buffered.next.right.get
            loop(buf)
          }
        }
        loop(ArrayBuffer())
      }
      val iterA: Iterator[A] = buffered.map(_.left.get)

      fn(iterA, bs)
    }
  }
}
