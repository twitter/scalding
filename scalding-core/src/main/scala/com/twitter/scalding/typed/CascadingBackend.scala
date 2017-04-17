package com.twitter.scalding.typed

import cascading.flow.FlowDef
import cascading.pipe.{ Each, Pipe }
import cascading.tuple.{ Fields, TupleEntry }
import com.twitter.scalding.TupleConverter.{ singleConverter, tuple2Converter }
import com.twitter.scalding.TupleSetter.{ singleSetter, tup2Setter }
import com.twitter.scalding._
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
  private[this] val pipeCache = new WeakHashMap[TypedPipe[Any], Map[Mode, CascadingPipe[Any]]]()

  private def cacheGet[T](t: TypedPipe[T], m: Mode)(p: FlowDef => CascadingPipe[T]): CascadingPipe[T] = {
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

  final def toPipe[U](p: TypedPipe[U], fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {

    def kvfields = Grouped.kvFields
    val f0 = new Fields(java.lang.Integer.valueOf(0))

    def singlePipe[T](t: TypedPipe[T], force: Boolean = false): CascadingPipe[T] =
      cacheGet(t, mode) { localFD =>
        val pipe = t.toPipe(f0)(localFD, mode, singleSetter)
        val p = if (force) RichPipe(pipe).forceToDisk else pipe
        CascadingPipe[T](p, f0, localFD, mode, singleConverter)
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
        // just use an empty iterable pipe.
        // Note, rest is irrelevant
        IterableSource(Iterable.empty, fieldNames)(setter, singleConverter[U]).read(flowDef, mode)

      case fk@FilterKeys(_, _) =>
        def go[K, V](node: FilterKeys[K, V]): Pipe = node match {
          case FilterKeys(IterablePipe(iter), fn) =>
            loop[(K, V)](IterablePipe(iter.filter { case (k, v) => fn(k) }), rest)
          case _ =>
            loop[(K, V)](node.input, rest.compose(FlatMapping.filterKeys(node.fn)))
        }
        go(fk)

      case f@Filter(_, _) =>
        // hand holding for type inference
        def go[T1 <: T](f: Filter[T1]) = f match {
          case Filter(IterablePipe(iter), fn) => loop(IterablePipe(iter.filter(fn)), rest)
          case _ =>
            loop[T1](f.input, rest.compose(FlatMapping.filter(f.fn)))
        }
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

      case ForceToDisk(EmptyTypedPipe) => loop(EmptyTypedPipe, rest)
      case ForceToDisk(i@IterablePipe(iter)) => loop(i, rest)
      case ForceToDisk(pipe) => loop(singlePipe(pipe, force = true), rest)

      case Fork(EmptyTypedPipe) => loop(EmptyTypedPipe, rest)
      case Fork(i@IterablePipe(iter)) => loop(i, rest)
      case Fork(pipe) => loop(singlePipe(pipe), rest)

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
        def sum[K, V](sblk: SumByLocalKeys[K, V]): CascadingPipe[(K, V)] =
          cacheGet(sblk, mode) { implicit localFD =>
            val pairPipe = sblk.input.toPipe(kvfields)(localFD, mode, tup2Setter)
            val msr = new MapsideReduce(sblk.semigroup, new Fields("key"), new Fields("value"), None)(singleConverter[V], singleSetter[V])
            val kvpipe = RichPipe(pairPipe).eachTo(kvfields -> kvfields) { _ => msr }
            CascadingPipe(kvpipe, kvfields, localFD, mode, tuple2Converter)
          }
        loop(sum(slk), rest)

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
          val cp = cacheGet(hcg, mode) { implicit fd =>
            val kvPipe = hcg.right.hashPipe(hcg.left)(hcg.joiner)(fd, mode)
            CascadingPipe(kvPipe, kvfields, fd, mode, tuple2Converter[K, R])
          }
          loop(cp, rest)
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
          loop(cp, rest)
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
          loop(cp, rest)
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

