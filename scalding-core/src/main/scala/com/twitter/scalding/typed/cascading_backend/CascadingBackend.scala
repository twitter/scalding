package com.twitter.scalding.typed.cascading_backend

import cascading.flow.FlowDef
import cascading.pipe.{ Each, Pipe }
import cascading.tuple.{ Fields, TupleEntry }
import com.twitter.scalding.TupleConverter.{ singleConverter, tuple2Converter }
import com.twitter.scalding.TupleSetter.{ singleSetter, tup2Setter }
import com.twitter.scalding.{ CleanupIdentityFunction, Dsl, LineNumber, IterableSource, MapsideReduce, Mode, RichPipe, TupleConverter, TupleSetter, Write }
import com.twitter.scalding.typed._
import com.twitter.scalding.serialization.{ CascadingBinaryComparator, OrderedSerialization, Boxed }
import scala.collection.mutable.{ Map => MMap }
import java.util.WeakHashMap

object CascadingBackend {
  import TypedPipe._

  /**
   * we want to cache renderings of some TypedPipe to Pipe so cascading
   * will see them as the same. Without this, it is very easy to have
   * a lot of recomputation. Ideally we would plan an entire graph
   * at once, and not need a static cache here, but currently we still
   * plan one TypedPipe at a time.
   */
  private class PipeCache {
    private[this] val pipeCache = new WeakHashMap[TypedPipe[Any], Map[Mode, CascadingPipe[Any]]]()

    def cacheGet[T](t: TypedPipe[T], m: Mode)(p: FlowDef => CascadingPipe[T]): CascadingPipe[T] = {
      def add(mmc: Map[Mode, CascadingPipe[Any]]): CascadingPipe[T] = {
        val emptyFD = new FlowDef
        val res = p(emptyFD)
        pipeCache.put(t, mmc + (m -> res.asInstanceOf[CascadingPipe[Any]]))
        res
      }

      pipeCache.synchronized {
        pipeCache.get(t) match {
          case null => add(Map.empty)
          case somemap if somemap.contains(m) => somemap(m).asInstanceOf[CascadingPipe[T]]
          case missing => add(missing)
        }
      }
    }
  }
  private[this] val pipeCache = new PipeCache

  final def toPipe[U](p: TypedPipe[U], fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {

    import pipeCache.cacheGet

    def kvfields = Grouped.kvFields
    val f0 = new Fields(java.lang.Integer.valueOf(0))

    def singlePipe[T](t: TypedPipe[T], force: Boolean = false): CascadingPipe[T] =
      cacheGet(t, mode) { localFD =>
        val pipe = t.toPipe(f0)(localFD, mode, singleSetter)
        val p = if (force) RichPipe(pipe).forceToDisk else pipe
        CascadingPipe[T](p, f0, localFD, mode, singleConverter)
      }

    def applyDescriptions(p: Pipe, descriptions: List[(String, Boolean)]): Pipe = {
      val ordered = descriptions.collect { case (d, false) => d }.reverse
      val unordered = descriptions.collect { case (d, true) => d }.distinct.sorted

      RichPipe.setPipeDescriptions(p, ordered ::: unordered)
    }

    def loop[T](t: TypedPipe[T], rest: FlatMappedFn[T, U], descriptions: List[(String, Boolean)]): Pipe = t match {
      case cp@CascadingPipe(_, fields, localFlowDef, m, _) =>
        import Dsl.flowDefToRichFlowDef
        // This check is not likely to fail unless someone does something really strange.
        // for historical reasons, it is not checked by the typed system
        require(m == mode,
          s"Cannot switch Mode between TypedSource.read and toPipe calls. Pipe: $p, pipe mode: $m, outer mode: $mode")
        flowDef.mergeFrom(localFlowDef)

        def go[T1 <: T](cp: CascadingPipe[T1]): Pipe = {
          val withRest = RichPipe(cp.pipe).flatMapTo[T1, U](fields -> fieldNames)(rest)(cp.converter, setter)
          applyDescriptions(withRest, descriptions)
        }
        go(cp)

      case cp@CrossPipe(_, _) => loop(cp.viaHashJoin, rest, descriptions)

      case cv@CrossValue(_, _) => loop(cv.viaHashJoin, rest, descriptions)

      case DebugPipe(p) =>
        // There is really little that can be done here but println
        loop(p.map { t => println(t); t }, rest, descriptions)

      case EmptyTypedPipe =>
        // just use an empty iterable pipe.
        // Note, rest is irrelevant
        val empty = IterableSource(Iterable.empty, fieldNames)(setter, singleConverter[U]).read(flowDef, mode)
        applyDescriptions(empty, descriptions)

      case fk@FilterKeys(_, _) =>
        def go[K, V](node: FilterKeys[K, V]): Pipe = node match {
          case FilterKeys(IterablePipe(iter), fn) =>
            loop[(K, V)](IterablePipe(iter.filter { case (k, v) => fn(k) }), rest, descriptions)
          case _ =>
            loop[(K, V)](node.input, rest.runAfter(FlatMapping.filterKeys(node.fn)), descriptions)
        }
        go(fk)

      case f@Filter(_, _) =>
        // hand holding for type inference
        def go[T1 <: T](f: Filter[T1]) = f match {
          case Filter(IterablePipe(iter), fn) => loop(IterablePipe(iter.filter(fn)), rest, descriptions)
          case _ =>
            loop[T1](f.input, rest.runAfter(FlatMapping.filter(f.fn)), descriptions)
        }
        go(f)

      case f@FlatMapValues(_, _) =>
        def go[K, V, U](node: FlatMapValues[K, V, U]): Pipe =
          loop(node.input, rest.runAfter(
            FlatMapping.FlatM[(K, V), (K, U)] { case (k, v) =>
              node.fn(v).map((k, _))
            }), descriptions)

        go(f)

      case FlatMapped(prev, fn) =>
        loop(prev, rest.runAfter(FlatMapping.FlatM(fn)), descriptions)

      case ForceToDisk(EmptyTypedPipe) => loop(EmptyTypedPipe, rest, descriptions)
      case ForceToDisk(i@IterablePipe(iter)) => loop(i, rest, descriptions)
      case ForceToDisk(pipe) => loop(singlePipe(pipe, force = true), rest, descriptions)

      case Fork(EmptyTypedPipe) => loop(EmptyTypedPipe, rest, descriptions)
      case Fork(i@IterablePipe(iter)) => loop(i, rest, descriptions)
      case Fork(pipe) => loop(singlePipe(pipe), rest, descriptions)

      case IterablePipe(iterable) =>
        val pipe = IterableSource(iterable, f0)(singleSetter[T], singleConverter[T]).read(flowDef, mode)
        val withRest = RichPipe(pipe).flatMapTo[T, U](f0 -> fieldNames)(rest)
        applyDescriptions(withRest, descriptions)

      case f@MapValues(_, _) =>
        def go[K, V, U](node: MapValues[K, V, U]): Pipe =
          loop(node.input, rest.runAfter(
            FlatMapping.Map[(K, V), (K, U)] { case (k, v) => (k, node.fn(v)) }), descriptions)

        go(f)

      case Mapped(input, fn) => loop(input, rest.runAfter(FlatMapping.Map(fn)), descriptions)

      case MergedTypedPipe(left, right) =>
        @annotation.tailrec
        def allMerged[A](m: TypedPipe[A],
          stack: List[TypedPipe[A]],
          acc: List[TypedPipe[A]],
          ds: List[(String, Boolean)]): (List[TypedPipe[A]], List[(String, Boolean)]) = m match {
            case MergedTypedPipe(left, right) =>
              allMerged(left, right :: stack, acc, ds)
            case EmptyTypedPipe => stack match {
              case Nil => (acc, ds)
              case h :: t => allMerged(h, t, acc, ds)
            }
            case WithDescriptionTypedPipe(p, desc, dedup) =>
              allMerged(p, stack, acc, (desc, dedup) :: ds)
            case notMerged =>
              allMerged(EmptyTypedPipe, stack, notMerged :: acc, ds)
          }
        val (unmerged, ds) = allMerged(left, right :: Nil, Nil, Nil)
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
          case Nil => loop(EmptyTypedPipe, rest, ds ::: descriptions)
          case h :: Nil => loop(h, rest, ds ::: descriptions)
          case otherwise =>
            // push all the remaining flatmaps up:
            val pipes = otherwise.map(loop(_, rest, Nil))
            // make the cascading pipe
            // TODO: a better optimization is to not materialize this
            // node at all if there is no fan out since groupBy and cogroupby
            // can accept multiple inputs
            val merged = new cascading.pipe.Merge(pipes.map(RichPipe.assignName): _*)
            applyDescriptions(merged, ds ::: descriptions)
        }
      case SourcePipe(source) =>
        val pipe = source.read(flowDef, mode)
        RichPipe(pipe).flatMapTo[T, U](source.sourceFields -> fieldNames)(rest)(source.converter, setter)

      case slk@SumByLocalKeys(_, _) =>
        def sum[K, V](sblk: SumByLocalKeys[K, V]): CascadingPipe[(K, V)] =
          cacheGet(sblk, mode) { implicit localFD =>
            val pairPipe = sblk.input.toPipe(kvfields)(localFD, mode, tup2Setter)
            val msr = new MapsideReduce(sblk.semigroup, new Fields("key"), new Fields("value"), None)(singleConverter[V], singleSetter[V])
            val kvpipe = RichPipe(pairPipe).eachTo(kvfields -> kvfields) { _ => msr }
            CascadingPipe(kvpipe, kvfields, localFD, mode, tuple2Converter)
          }
        loop(sum(slk), rest, descriptions)

      case tp@TrappedPipe(_, _, _) =>
        def go[T0, T1 >: T0](tp: TrappedPipe[T0, T1], r: FlatMappedFn[T1, U]): Pipe = {
          val cp = cacheGet(tp, mode) { implicit fd =>
            val sfields = tp.sink.sinkFields
            // TODO: with diamonds in the graph, this might not be correct
            val pp = tp.input.toPipe[T0](sfields)(fd, mode, tp.sink.setter)
            val pipe = RichPipe.assignName(pp)
            flowDef.addTrap(pipe, tp.sink.createTap(Write)(mode))
            CascadingPipe[T1](pipe, sfields, fd, mode, tp.conv)
          }
          loop(cp, r, descriptions)
        }
        go(tp, rest)

      case WithDescriptionTypedPipe(pipe, description, dedup) =>
        loop(pipe, rest, (description, dedup) :: descriptions)

      case WithOnComplete(pipe, fn) =>
        val planned = loop(pipe, rest, descriptions)
        new Each(planned, Fields.ALL, new CleanupIdentityFunction(fn), Fields.REPLACE)

      case hcg@HashCoGroup(_, _, _) =>
        def go[K, V1, V2, R](hcg: HashCoGroup[K, V1, V2, R]): Pipe = {
          // TODO we can push up filterKeys on both the left and right
          // and mapValues/flatMapValues on the result
          val cp = cacheGet(hcg, mode) { implicit fd =>
            val kvPipe = hcg.right.hashPipe(hcg.left)(hcg.joiner)(fd, mode)
            CascadingPipe(kvPipe, kvfields, fd, mode, tuple2Converter[K, R])
          }
          loop(cp, rest, descriptions)
        }
        go(hcg)

      case cgp@CoGroupedPipe(_) =>
        def go[K, V](cgp: CoGroupedPipe[K, V]): Pipe = {
          // TODO we can push up filterKeys on both the left and right
          // and mapValues/flatMapValues on the result
          val cp = cacheGet(cgp, mode) { implicit fd =>
            val kvPipe = cgp.cogrouped.toKVPipe(fd, mode)
            CascadingPipe(kvPipe, kvfields, fd, mode, tuple2Converter[K, V])
          }
          loop(cp, rest, descriptions)
        }
        go(cgp)

      case r@ReduceStepPipe(_, _, _) =>
        def go[K, V1, V2](rsp: ReduceStepPipe[K, V1, V2]): Pipe = {
          // TODO we can push up filterKeys on both the left and right
          // and mapValues/flatMapValues on the result

          def pipe(flowDef: FlowDef) = Grouped.maybeBox[K, V1](rsp.reduce.keyOrdering, flowDef) { (tupleSetter, fields) =>
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

          val cp = cacheGet(rsp, mode) { implicit fd =>
            val tupConv = Grouped.tuple2Conv[K, V2](rsp.reduce.keyOrdering)
            CascadingPipe(pipe(fd), kvfields, fd, mode, tupConv)
          }
          loop(cp, rest, descriptions)
      }
      go(r)
    }

    RichPipe(loop(p, FlatMappedFn.identity[U], Nil)).applyFlowConfigProperties(flowDef)
  }
}

