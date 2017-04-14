package com.twitter.scalding.typed

import cascading.flow.FlowDef
import cascading.pipe.{ Each, Pipe }
import cascading.tuple.{ Fields, TupleEntry }
import com.twitter.scalding.TupleConverter.{ singleConverter, tuple2Converter }
import com.twitter.scalding.TupleSetter.{ singleSetter, tup2Setter }
import com.twitter.scalding._
import com.twitter.scalding.serialization.{ CascadingBinaryComparator, OrderedSerialization, Boxed }
import scala.collection.mutable.{ Map => MMap }

object CascadingBackend {
  import TypedPipe._


  final def toPipe[U](p: TypedPipe[U], fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {

    val kvfields = Grouped.kvFields
    val kvcache = MMap[TypedPipe[(Any, Any)], Pipe]()
    def kvPipe[K, V](kv: TypedPipe[(K, V)]): Pipe =
      kvcache.getOrElseUpdate(kv, kv.toPipe(kvfields)(flowDef, mode, tup2Setter))

    val f0 = new Fields(java.lang.Integer.valueOf(0))
    val cache = MMap[TypedPipe[Any], Pipe]()
    def singlePipe[T](t: TypedPipe[T], onPipe: Pipe => Pipe): (Pipe, TypedPipe[T]) = {
      val pipe = cache.getOrElseUpdate(t, t.toPipe[T](f0)(flowDef, mode, singleSetter))
      (pipe, TypedPipe.fromSingleField[T](onPipe(pipe))(flowDef, mode))
    }

    def loop[T](t: TypedPipe[T], rest: FlatMappedFn[T, U]): Pipe = t match {
      case cp@CascadingPipe(_, fields, localFlowDef, m, _) =>
        import Dsl.flowDefToRichFlowDef
        // This check is not likely to fail unless someone does something really strange.
        // for historical reasons, it is not checked by the typed system
        require(m == mode,
          s"Cannot switch Mode between TypedSource.read and toPipe calls. Pipe: $p, pipe mode: $m, outer mode: $mode")
        flowDef.mergeFrom(localFlowDef)

        def go[T1 <: T](cp: CascadingPipe[T1]): Pipe = {
          RichPipe(cp.pipe).flatMapTo[T1, U](fields -> fieldNames)(rest)(cp.converter, setter)
        }
        go(cp)

      case cp@CrossPipe(_, _) => loop(cp.viaHashJoin, rest)

      case cv@CrossValue(_, _) => loop(cv.viaHashJoin, rest)

      case DebugPipe(p) =>
        // There is really little that can be done here but println
        loop(p.map { t => println(t); t }, rest)

      case EmptyTypedPipe =>
        // just use an empty iterable pipe
        IterableSource(Iterable.empty, fieldNames)(setter, singleConverter[U]).read(flowDef, mode)

      case fk@FilterKeys(_, _) =>
        def go[K, V](node: FilterKeys[K, V]): Pipe =
          loop[(K, V)](node.input, rest.compose(FlatMapping.filterKeys(node.fn)))
        go(fk)

      case f@Filter(_, _) =>
        // hand holding for type inference
        def go[T1 <: T](f: Filter[T1]) = loop[T1](f.input, rest.compose(FlatMapping.filter(f.fn)))
        go(f)
      case f@FlatMapValues(_, _) =>
        def go[K, V, U](node: FlatMapValues[K, V, U]): Pipe =
          loop(node.input, rest.compose(
            FlatMapping.FlatM[(K, V), (K, U)] { case (k, v) =>
              node.fn(v).map((k, _))
            }))

        go(f)

      case FlatMapped(prev, fn) =>
        loop(prev, rest.compose(FlatMapping.FlatM(fn)))

      case ForceToDisk(pipe) =>
        val (_, prev) = singlePipe(pipe, { p => RichPipe(p).forceToDisk })
        loop(prev, rest)
      case Fork(pipe) =>
        val (_, prev) = singlePipe(pipe, identity)
        loop(prev, rest)
      case IterablePipe(iterable) =>
        val pipe = IterableSource(iterable, f0)(singleSetter[T], singleConverter[T]).read(flowDef, mode)
        RichPipe(pipe).flatMapTo[T, U](f0 -> fieldNames)(rest)
      case f@MapValues(_, _) =>
        def go[K, V, U](node: MapValues[K, V, U]): Pipe =
          loop(node.input, rest.compose(
            FlatMapping.Map[(K, V), (K, U)] { case (k, v) => (k, node.fn(v)) }))

        go(f)
      case Mapped(input, fn) => loop(input, rest.compose(FlatMapping.Map(fn)))
      case MergedTypedPipe(left, right) =>
        @annotation.tailrec
        def allMerged[A](m: TypedPipe[A], stack: List[TypedPipe[A]], acc: List[TypedPipe[A]]): List[TypedPipe[A]] = m match {
          case MergedTypedPipe(left, right) => allMerged(left, right :: stack, acc)
          case EmptyTypedPipe => stack match {
            case Nil => acc
            case h :: t => allMerged(h, t, acc)
          }
          case notMerged => allMerged(EmptyTypedPipe, stack, notMerged :: acc)
        }
        val unmerged = allMerged(left, right :: Nil, Nil)
        // check for repeated pipes
        val uniquePipes: List[TypedPipe[T]] = unmerged
          .groupBy(identity)
          .mapValues(_.size)
          .map {
            case (pipe, 1) => pipe
            case (pipe, cnt) => pipe.flatMap(List.fill(cnt)(_).iterator)
          }
          .toList

        uniquePipes match {
          case Nil => loop(EmptyTypedPipe, rest)
          case h :: Nil => loop(h, rest)
          case otherwise =>
            // push all the remaining flatmaps up:
            val pipes = otherwise.map(loop(_, rest))
            // make the cascading pipe
            // TODO: a better optimization is to not materialize this
            // node at all if there is no fan out since groupBy and cogroupby
            // can accept multiple inputs
            new cascading.pipe.Merge(pipes.map(RichPipe.assignName): _*)
        }
      case SourcePipe(source) =>
        val pipe = source.read(flowDef, mode)
        RichPipe(pipe).flatMapTo[T, U](source.sourceFields -> fieldNames)(rest)(source.converter, setter)
      case slk@SumByLocalKeys(_, _) =>
        def sum[K, V](sblk: SumByLocalKeys[K, V]): Pipe = {
          val pipe = kvPipe(sblk.input)
          val msr = new MapsideReduce(sblk.semigroup, new Fields("key"), new Fields("value"), None)(singleConverter[V], singleSetter[V])
          val kvpipe = RichPipe(pipe).eachTo(kvfields -> kvfields) { _ => msr }
          RichPipe(kvpipe).flatMapTo[(K, V), U](kvfields -> fieldNames)(rest)(tuple2Converter, setter)
        }
        sum(slk)
      case tp@TrappedPipe(_, _, _) =>
        def go[T0, T1 >: T0](tp: TrappedPipe[T0, T1], r: FlatMappedFn[T1, U]): Pipe = {
          val sfields = tp.sink.sinkFields
          // TODO: with diamonds in the graph, this might not be correct
          val pp = tp.input.toPipe[T0](sfields)(flowDef, mode, tp.sink.setter)
          val pipe = RichPipe.assignName(pp)
          flowDef.addTrap(pipe, tp.sink.createTap(Write)(mode))
          val cp = CascadingPipe[T1](pipe, sfields, flowDef, mode, tp.conv)
          loop(cp, r)
        }
        go(tp, rest)
      case WithDescriptionTypedPipe(pipe, description) =>
        val planned = loop(pipe, rest)
        RichPipe.setPipeDescriptions(planned, List(description))
      case WithOnComplete(pipe, fn) =>
        val planned = loop(pipe, rest)
        new Each(planned, Fields.ALL, new CleanupIdentityFunction(fn), Fields.REPLACE)
      case hcg@HashCoGroup(_, _, _) =>
        def go[K, V1, V2, R](hcg: HashCoGroup[K, V1, V2, R]): Pipe = {
          // TODO we can push up filterKeys on both the left and right
          // and mapValues/flatMapValues on the result
          val kvPipe = kvcache.getOrElseUpdate(hcg, hcg.right.hashPipe(hcg.left)(hcg.joiner))
          loop(CascadingPipe(kvPipe, kvfields, flowDef, mode, tuple2Converter[K, R]), rest)
        }
        go(hcg)
      case cgp@CoGroupedPipe(_) =>
        def go[K, V](cgp: CoGroupedPipe[K, V]): Pipe = {
          // TODO we can push up filterKeys on both the left and right
          // and mapValues/flatMapValues on the result
          val kvPipe = kvcache.getOrElseUpdate(cgp, cgp.cogrouped.toKVPipe(flowDef, mode))
          loop(CascadingPipe(kvPipe, kvfields, flowDef, mode, tuple2Converter[K, V]), rest)
        }
        go(cgp)
      case r@ReduceStepPipe(_, _, _) =>
        def go[K, V1, V2](rsp: ReduceStepPipe[K, V1, V2]): Pipe = {
          // TODO we can push up filterKeys on both the left and right
          // and mapValues/flatMapValues on the result

          def pipe = Grouped.maybeBox[K, V1](rsp.reduce.keyOrdering, flowDef) { (tupleSetter, fields) =>
            val (sortOpt, ts) = rsp.valueOrd.map {
              case ordser: OrderedSerialization[V1] =>
                // We get in here when we do a secondary sort
                // and that sort is an ordered serialization
                // We now need a boxed serializer for this type
                // Then we set the comparator on the field, and finally we box the value with our tupleSetter
                val (boxfn, boxordSer) = Grouped.getBoxFnAndOrder[V1](ordser, flowDef)
                val valueF = new Fields("value")
                valueF.setComparator("value", new CascadingBinaryComparator(boxordSer))
                val ts2 = tupleSetter.asInstanceOf[TupleSetter[(K, Boxed[V1])]].contraMap { kv1: (K, V1) => (kv1._1, boxfn(kv1._2)) }
                (Some(valueF), ts2)
              case vs =>
                (Some(Grouped.valueSorting(vs)), tupleSetter)
            }.getOrElse((None, tupleSetter))

            val p = rsp.reduce.mapped.toPipe(Grouped.kvFields)(flowDef, mode, TupleSetter.asSubSetter(ts))

            RichPipe(p).groupBy(fields) { inGb =>
                val withSort = sortOpt.fold(inGb)(inGb.sortBy)
                rsp.op(withSort)
              }
          }

          val kvPipe = kvcache.getOrElseUpdate(r, pipe)
          val tupConv = Grouped.tuple2Conv[K, V2](rsp.reduce.keyOrdering)
          loop(CascadingPipe(kvPipe, kvfields, flowDef, mode, tupConv), rest)
      }
      go(r)
    }

    import Dsl._ // Ensure we hook into all pipes coming out of the typed API to apply the FlowState's properties on their pipes
    val pipe =
      loop(p, FlatMappedFn.identity[U])
        .applyFlowConfigProperties(flowDef)
    RichPipe.setPipeDescriptionFrom(pipe, LineNumber.tryNonScaldingCaller)
  }
}

