package com.twitter.scalding.typed.cascading_backend

import cascading.flow.FlowDef
import cascading.operation.Debug
import cascading.pipe.{ CoGroup, Each, Pipe, HashJoin }
import cascading.tuple.{ Fields, Tuple => CTuple }
import com.stripe.dagon.{ FunctionK, HCache, Id, Rule, Dag }
import com.twitter.scalding.TupleConverter.{ singleConverter, tuple2Converter }
import com.twitter.scalding.TupleSetter.{ singleSetter, tup2Setter }
import com.twitter.scalding.{
  CleanupIdentityFunction, Config, Dsl, Execution, Field, FlowState, FlowStateMap, GroupBuilder,
  HadoopMode, IncrementCounters, IterableSource, MapsideReduce, Mode, RichFlowDef,
  RichPipe, TupleConverter, TupleGetter, TupleSetter, TypedBufferOp, WrappedJoiner, Write
}
import com.twitter.scalding.typed._
import com.twitter.scalding.typed.functions.{ FilterKeysToFilter, MapValuesToMap, FlatMapValuesToFlatMap }
import com.twitter.scalding.serialization.{
  Boxed,
  BoxedOrderedSerialization,
  CascadingBinaryComparator,
  EquivSerialization,
  OrderedSerialization,
  WrappedSerialization
}
import java.util.WeakHashMap
import org.slf4j.LoggerFactory

object CascadingBackend {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  def areDefiniteInverse[A, B](t: TupleConverter[A], s: TupleSetter[B]): Boolean =
    (t, s) match {
      case (TupleConverter.Single(TupleGetter.Casting()), TupleSetter.Single()) => true
      case (TupleConverter.TupleConverter1(TupleGetter.Casting()), TupleSetter.TupleSetter1()) => true
      case (TupleConverter.TupleConverter2(TupleGetter.Casting(), TupleGetter.Casting()), TupleSetter.TupleSetter2()) => true
      // TODO we could add more, but we only use single and 2 actually
      case _ => false
    }

  import TypedPipe._

  private val valueField: Fields = new Fields("value")
  private val kvFields: Fields = new Fields("key", "value")
  private val f0: Fields = new Fields(java.lang.Integer.valueOf(0))

  private def tuple2Conv[K, V](ord: Ordering[K]): TupleConverter[(K, V)] =
    ord match {
      case _: OrderedSerialization[_] =>
        tuple2Converter[Boxed[K], V].andThen { kv =>
          (kv._1.get, kv._2)
        }
      case _ => tuple2Converter[K, V]
    }

  private def valueConverter[V](optOrd: Option[Ordering[V]]): TupleConverter[V] =
    optOrd.map {
      case _: OrderedSerialization[_] =>
        TupleConverter.singleConverter[Boxed[V]].andThen(_.get)
      case _ => TupleConverter.singleConverter[V]
    }.getOrElse(TupleConverter.singleConverter[V])

  private def keyConverter[K](ord: Ordering[K]): TupleConverter[K] =
    ord match {
      case _: OrderedSerialization[_] =>
        TupleConverter.singleConverter[Boxed[K]].andThen(_.get)
      case _ => TupleConverter.singleConverter[K]
    }

  private def keyGetter[K](ord: Ordering[K]): TupleGetter[K] =
    ord match {
      case _: OrderedSerialization[K] =>
        new TupleGetter[K] {
          def get(tup: CTuple, i: Int) = tup.getObject(i).asInstanceOf[Boxed[K]].get
        }
      case _ => TupleGetter.castingGetter
    }

  /**
   * If we are using OrderedComparable, we need to box the key
   * to prevent other serializers from handling the key
   */
  private def getBoxFnAndOrder[K](ordser: OrderedSerialization[K], flowDef: FlowDef): (K => Boxed[K], BoxedOrderedSerialization[K]) = {
    // We can only supply a cacheKey if the equals and hashcode are known sane
    val (boxfn, cls) = Boxed.nextCached[K](if (ordser.isInstanceOf[EquivSerialization[_]]) Some(ordser) else None)
    val boxordSer = BoxedOrderedSerialization(boxfn, ordser)

    WrappedSerialization.rawSetBinary(List((cls, boxordSer)),
      {
        case (k: String, v: String) =>
          FlowStateMap.merge(flowDef, FlowState.withConfigSetting(k + cls, v))
      })
    (boxfn, boxordSer)
  }

  /**
   * Check if the Ordering is an OrderedSerialization, if so box in a Boxed so hadoop and cascading
   * can dispatch the right serialization
   */
  private def maybeBox[K, V](ord: Ordering[K], flowDef: FlowDef)(op: (TupleSetter[(K, V)], Fields) => Pipe): Pipe =
    ord match {
      case ordser: OrderedSerialization[K] =>
        val (boxfn, boxordSer) = getBoxFnAndOrder[K](ordser, flowDef)

        val ts = tup2Setter[(Boxed[K], V)].contraMap { kv1: (K, V) => (boxfn(kv1._1), kv1._2) }
        val keyF = new Fields("key")
        keyF.setComparator("key", new CascadingBinaryComparator(boxordSer))
        op(ts, keyF)
      case _ =>
        val ts = tup2Setter[(K, V)]
        val keyF = Field.singleOrdered("key")(ord)
        op(ts, keyF)
    }

  // TODO we could probably optimize this further by just composing
  // the toPipe function directly, so we don't actually create the pipe until
  // the TupleSetter comes in. With this, we can make sure to use the right
  // TupleSetter on the final pipe
   private case class CascadingPipe[+T](pipe: Pipe,
    fields: Fields,
    @transient localFlowDef: FlowDef, // not serializable.
    converter: TupleConverter[_ <: T]) {

    /**
     * merge the flowDef into this new flowdef an make sure the tuples
     * have the structure defined by setter
     */
    def toPipe[U >: T](f: Fields, fd: FlowDef, setter: TupleSetter[U]): Pipe = {
      val resFd = new RichFlowDef(fd)
      resFd.mergeFrom(localFlowDef)

      if (!RichPipe.isPassthrough(pipe) && areDefiniteInverse(converter, setter) && (fields == f)) {
        // Note that some custom scalding sources have a bug
        // that the fields on the TypedSource don't match the fields on underlying
        // scheme. To work around this, we don't do this optimization on sources.
        //
        // We have extended this to cover filters, since we can push those down
        // and they will proxy through the previous fields which may be UNKNOWN
        //
        // Note, cascading appears to not care if it sees UNKNOWN fields as input to
        // a filter or map, but it does care about creating a Merge with
        // unknown fields, since we always call toPipe before creating the Merge,
        // as long as the chain of operations contains at least one non-filter,
        // we should be okay
        //
        // in this branch, the TupleConverter is what the Setter is expecting, so we don't
        // need to do any transform
        // we are already in the right format
        pipe
      } else {
        // we need to convert
        RichPipe(pipe).mapTo[T, U](fields -> f)(t => t)(TupleConverter.asSuperConverter(converter), setter)
      }
    }
  }

  private object CascadingPipe {
    def single[T](pipe: Pipe, fd: FlowDef): CascadingPipe[T] =
      CascadingPipe(pipe, f0, fd, singleConverter[T])
  }

  /**
   * we want to cache renderings of some TypedPipe to Pipe so cascading
   * will see them as the same. Without this, it is very easy to have
   * a lot of recomputation. Ideally we would plan an entire graph
   * at once, and not need a static cache here, but currently we still
   * plan one TypedPipe at a time.
   */
  private class CompilerCache {

    private[this] val cache = new WeakHashMap[FlowDef, FunctionK[TypedPipe, CascadingPipe]]()

    def get(fd: FlowDef, m: Mode): FunctionK[TypedPipe, CascadingPipe] =
      cache.synchronized {
        cache.get(fd) match {
          case null =>
            val c = compile(m)
            cache.put(fd, c)
            c
          case nonNull => nonNull
        }
      }
  }
  private[this] val cache = new CompilerCache

  /**
   * Method to compile scalding's `TypedPipe`s to cascading's `Pipe`s.
   *
   * Since equal `TypedPipe`s define same computation we would like to compile them into referentially the same cascading's `Pipe` instance.
   * This logic breaks if one typed pipe is really big and has two forked different computations both of which significantly decrease size of the data.
   * If we will cache common part of this two computations in the same cascading's `Pipe` instance we end up with common part being materialized.
   * Therefore for some kind of `TypedPipe`s we want to avoid their caching.
   *
   * `.cross` `TypedPipe` is one of examples of such `TypedPipe`s we never want to materialize and, therefore, cache.
   *
   * `compile` logic is separated into next functions:
   * - `transform` which defines main transformation logic, without any caching applied.
   *   This method accepts `rec` parameter which is being called to transform children pipes.
   * - `withCachePolicy` which defines transformation logic with caching applied.
   * - `notCached` to support use case with `.cross` pipe, where pipe itself shouldn't be cached but `left` and `right` sides of it should be.
   */
  private def compile(mode: Mode): FunctionK[TypedPipe, CascadingPipe] =
    new FunctionK[TypedPipe, CascadingPipe] {

      private val cache = HCache.empty[TypedPipe, CascadingPipe]

      override def toFunction[U]: TypedPipe[U] => CascadingPipe[U] = withCachePolicy

      private def withCachePolicy[U]: TypedPipe[U] => CascadingPipe[U] = {
        // Don't cache `CrossPipe`, but cache `left` and `right` side of it
        case cp@CrossPipe(left, right) =>
          notCached(excludes = Set(left, right))(cp)
        // Don't cache `Fork` and `WithDescriptionTypedPipe`
        // since if we do cache them `CrossPipe` will end up being cached as well
        case tp@Fork(_) =>
          transform(tp, this)
        case tp@WithDescriptionTypedPipe(_, _) =>
          transform(tp, this)
        // Cache all other typed pipes
        case tp =>
          cache.getOrElseUpdate(tp, transform(tp, this))
      }

      private def notCached(excludes: Set[TypedPipe[_]]): FunctionK[TypedPipe, CascadingPipe] =
        new FunctionK[TypedPipe, CascadingPipe] {
          override def toFunction[U]: TypedPipe[U] => CascadingPipe[U] = { tp =>
            if (excludes.contains(tp)) withCachePolicy(tp) else transform(tp, this)
          }
        }

      private def transform[T](
        pipe: TypedPipe[T],
        rec: FunctionK[TypedPipe, CascadingPipe]
      ): CascadingPipe[T] = pipe match {
        case cp@CounterPipe(_) =>
          def go[A](cp: CounterPipe[A]): CascadingPipe[A] = {
            val CascadingPipe(pipe0, initF, fd, conv) = rec(cp.pipe)
            val cpipe = RichPipe(pipe0)
              .eachTo(initF -> f0)(new IncrementCounters[A](_, TupleConverter
                .asSuperConverter(conv)))
            CascadingPipe.single[A](cpipe, fd)
          }

          go(cp)
        case cp@CrossPipe(_, _) =>
          rec(cp.viaHashJoin)
        case cv@CrossValue(_, _) =>
          rec(cv.viaHashJoin)
        case DebugPipe(p) =>
          val inner = rec(p)
          inner.copy(pipe = new Each(inner.pipe, new Debug))
        case EmptyTypedPipe =>
          // just use an empty iterable pipe.
          rec(IterablePipe(List.empty[T]))
        case fk@FilterKeys(_, _) =>
          def go[K, V](node: FilterKeys[K, V]): CascadingPipe[(K, V)] = {
            val rewrite = Filter[(K, V)](node.input, FilterKeysToFilter(node.fn))
            rec(rewrite)
          }

          go(fk)
        case f@Filter(_, _) =>
          // hand holding for type inference
          def go[T1 <: T](f: Filter[T1]): CascadingPipe[T] = {
            val Filter(input, fn) = f
            val CascadingPipe(pipe, initF, fd, conv) = rec(input)
            // This does not need a setter, which is nice.
            val fpipe = RichPipe(pipe).filter[T1](initF)(fn)(TupleConverter.asSuperConverter(conv))
            CascadingPipe[T](fpipe, initF, fd, conv)
          }

          go(f)
        case f@FlatMapValues(_, _) =>
          def go[K, V, U](node: FlatMapValues[K, V, U]): CascadingPipe[T] =
            rec(FlatMapped[(K, V), (K, U)](node.input, FlatMapValuesToFlatMap(node.fn)))

          go(f)
        case fm@FlatMapped(_, _) =>
          // TODO we can optimize a flatmapped input directly and skip some tupleconverters
          def go[A, B <: T](fm: FlatMapped[A, B]): CascadingPipe[T] = {
            val CascadingPipe(pipe, initF, fd, conv) = rec(fm.input)
            val fmpipe = RichPipe(pipe).flatMapTo[A, T](initF -> f0)(fm.fn)(TupleConverter
              .asSuperConverter(conv), singleSetter)
            CascadingPipe.single[B](fmpipe, fd)
          }

          go(fm)
        case ForceToDisk(input) =>
          val cp = rec(input)
          cp.copy(pipe = RichPipe(cp.pipe).forceToDisk)
        case Fork(input) =>
          // fork doesn't mean anything here since we are already planning each TypedPipe to
          // something in cascading. Fork is an optimizer level operation
          rec(input)
        case IterablePipe(iter) =>
          val fd = new FlowDef
          val pipe = IterableSource[T](iter, f0)(singleSetter, singleConverter).read(fd, mode)
          CascadingPipe.single[T](pipe, fd)
        case f@MapValues(_, _) =>
          def go[K, A, B](fn: MapValues[K, A, B]): CascadingPipe[_ <: (K, B)] =
            rec(Mapped[(K, A), (K, B)](fn.input, MapValuesToMap(fn.fn)))

          go(f)
        case m@Mapped(_, _) =>
          def go[A, B <: T](m: Mapped[A, B]): CascadingPipe[T] = {
            val Mapped(input, fn) = m
            val CascadingPipe(pipe, initF, fd, conv) = rec(input)
            val fmpipe = RichPipe(pipe).mapTo[A, T](initF -> f0)(fn)(TupleConverter
              .asSuperConverter(conv), singleSetter)
            CascadingPipe.single[B](fmpipe, fd)
          }

          go(m)

        case m@MergedTypedPipe(_, _) =>
          OptimizationRules.unrollMerge(m) match {
            case Nil => rec(EmptyTypedPipe)
            case h :: Nil => rec(h)
            case nonEmpty =>
              // TODO: a better optimization is to not materialize this
              // node at all if there is no fan out since groupBy and cogroupby
              // can accept multiple inputs

              val flowDef = new FlowDef
              // if all of the converters are the same, we could skip some work
              // here, but need to be able to see that correctly
              val pipes = nonEmpty.map { p => rec(p).toPipe(f0, flowDef, singleSetter) }
              val merged = new cascading.pipe.Merge(pipes.map(RichPipe.assignName): _*)
              CascadingPipe.single[T](merged, flowDef)
          }
        case SourcePipe(typedSrc) =>
          val fd = new FlowDef
          val pipe = typedSrc.read(fd, mode)
          CascadingPipe[T](pipe, typedSrc.sourceFields, fd, typedSrc.converter[T])
        case sblk@SumByLocalKeys(_, _) =>
          def go[K, V](sblk: SumByLocalKeys[K, V]): CascadingPipe[(K, V)] = {
            val cp = rec(sblk.input)
            val localFD = new FlowDef
            val cpKV: Pipe = cp.toPipe(kvFields, localFD, tup2Setter)
            val msr = new MapsideReduce(sblk
              .semigroup, new Fields("key"), valueField, None)(singleConverter[V], singleSetter[V])
            val kvpipe = RichPipe(cpKV).eachTo(kvFields -> kvFields) { _ => msr }
            CascadingPipe(kvpipe, kvFields, localFD, tuple2Converter[K, V])
          }

          go(sblk)
        case trapped: TrappedPipe[u] =>
          val cp = rec(trapped.input)
          import trapped._
          // TODO: with diamonds in the graph, this might not be correct
          // it seems cascading requires puts the immediate tuple that
          // caused the exception, so if you addTrap( ).map(f).map(g)
          // and f changes the tuple structure, if we don't collapse the
          // maps into 1 operation, cascading can write two different
          // schemas into the trap, making it unreadable.
          // this basically means there can only be one operation in between
          // a trap and a forceToDisk or a groupBy/cogroupBy (any barrier).
          val fd = new FlowDef
          val pp: Pipe = cp.toPipe[u](sink.sinkFields, fd, TupleSetter.asSubSetter(sink.setter))
          val pipe = RichPipe.assignName(pp)
          fd.addTrap(pipe, sink.createTap(Write)(mode))
          CascadingPipe[u](pipe, sink.sinkFields, fd, conv)
        case WithDescriptionTypedPipe(input, descs) =>

          @annotation.tailrec
          def loop[A](
            t: TypedPipe[A],
            acc: List[(String, Boolean)]
          ): (TypedPipe[A], List[(String, Boolean)]) =
            t match {
              case WithDescriptionTypedPipe(i, descs) =>
                loop(i, descs ::: acc)
              case notDescr => (notDescr, acc)
            }

          val (root, allDesc) = loop(input, descs)
          val cp = rec(root)
          cp.copy(pipe = applyDescriptions(cp.pipe, allDesc))

        case WithOnComplete(input, fn) =>
          val cp = rec(input)
          val next = new Each(cp.pipe, Fields.ALL, new CleanupIdentityFunction(fn))
          cp.copy(pipe = next)

        case hcg@HashCoGroup(_, _, _) =>
          def go[K, V1, V2, R](hcg: HashCoGroup[K, V1, V2, R]): CascadingPipe[(K, R)] =
            planHashJoin(hcg.left,
              hcg.right,
              hcg.joiner,
              rec)

          go(hcg)
        case ReduceStepPipe(rs) =>
          planReduceStep(rs, rec)

        case CoGroupedPipe(cg) =>
          planCoGroup(cg, rec)
      }
    }

  private def applyDescriptions(p: Pipe, descriptions: List[(String, Boolean)]): Pipe = {
    val ordered = descriptions.collect { case (d, false) => d }.reverse
    val unordered = descriptions.collect { case (d, true) => d }.distinct.sorted

    RichPipe.setPipeDescriptions(p, ordered ::: unordered)
  }

  /**
   * These are rules we should apply to any TypedPipe before handing
   * to cascading. These should be a bit conservative in that they
   * should be highly likely to improve the graph.
   */
  def defaultOptimizationRules(config: Config): Seq[Rule[TypedPipe]] = {

    def std(forceHash: Rule[TypedPipe]) =
      (OptimizationRules.standardMapReduceRules :::
        List(
          OptimizationRules.FilterLocally, // after filtering, we may have filtered to nothing, lets see
          OptimizationRules.simplifyEmpty,
          // add any explicit forces to the optimized graph
          Rule.orElse(List(
            forceHash,
            OptimizationRules.RemoveDuplicateForceFork)
          )))

    config.getOptimizationPhases match {
      case Some(tryPhases) => tryPhases.get.phases
      case None =>
        val force =
          if (config.getHashJoinAutoForceRight) OptimizationRules.ForceToDiskBeforeHashJoin
          else Rule.empty[TypedPipe]
        std(force)
    }
  }

  final def toPipe[U](p: TypedPipe[U], fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {

    val phases = defaultOptimizationRules(
      mode match {
        case h: HadoopMode => Config.fromHadoop(h.jobConf)
        case _ => Config.empty
      })
    val (d, id) = Dag(p, OptimizationRules.toLiteral)
    val d1 = d.applySeq(phases)
    val p1 = d1.evaluate(id)

    // Now that we have an optimized pipe, convert it to a Pipe
    toPipeUnoptimized(p1, fieldNames)
  }

  /**
   * This needs to be called, to plan any writes from the
   * TypedPipe API. This is here to globally optimize the entire
   * flow, and not optimize on a write-by-write basis.
   *
   * This uses the FlowStateMap this method is idempotent.
   *
   * It is called by default in ExecutionContext at the last
   * step before building the Flow. Job also needs to call this
   * method in validate to make sure validation works.
   */
  def planTypedWrites(fd: FlowDef, mode: Mode): Unit = {
    def doWrites(writes: List[FlowStateMap.TypedWrite[_]]): Unit = {
      val empty = Dag.empty(OptimizationRules.toLiteral)
      type ToDo[A] = (Id[A], TypedSink[A])
      val (rootedDag, todos) = writes.foldLeft((empty, List.empty[ToDo[_]])) { case ((dag, items), tw) =>
        val (nextDag, id) = dag.addRoot(tw.pipe)
        require(tw.mode == mode, s"${tw.mode} should be equal to $mode")
        (nextDag, (id, tw.sink) :: items)
      }
      val phases = defaultOptimizationRules(
        mode match {
          case h: HadoopMode => Config.fromHadoop(h.jobConf)
          case _ => Config.empty
        })
      val optDag = rootedDag.applySeq(phases)
      def doWrite[A](pair: (Id[A], TypedSink[A])): Unit = {
        val optPipe = optDag.evaluate(pair._1)
        val dest = pair._2
        val cpipe = toPipeUnoptimized[A](optPipe, dest.sinkFields)(fd, mode, dest.setter)
        dest.writeFrom(cpipe)(fd, mode)
      }
      todos.foreach(doWrite(_))
    }

    doWrites(FlowStateMap.removeWrites(fd).pendingTypedWrites)

    // We do writes twice because customer can use Typed API in their TypedSink implementation.
    val pendingWritesAfterPlan = FlowStateMap.removeWrites(fd).pendingTypedWrites
    if (pendingWritesAfterPlan.nonEmpty) {
      logger.warn("Using Typed API in TypedSink implementation is prohibited and " +
        "might be removed in later releases of Scalding.")
      doWrites(pendingWritesAfterPlan)
    }

    if (FlowStateMap.removeWrites(fd).pendingTypedWrites.nonEmpty) {
      throw new IllegalStateException("You can't use Typed API in you TypedSink implementation nestedly.")
    }
  }

  /**
   * Convert a cascading FlowDef to an Option[Execution[Unit]]
   *
   * Return None if the Execution is empty. If the FlowDef includes
   * things other than TypedPipe.writes, this will return Some
   * failed Execution.
   *
   * This method is useful for people who have used the Typed-API
   * with FlowDefs, but not Executions and want to convert to
   * an Execution without rewriting all the code. An example
   * of this is summingbird which constructs a single FlowDef
   * for the entire plan it makes. For large plans from summingbird
   * you may want to use write partitioning.
   */
  def flowDefToExecution(fd: FlowDef, partitionOptimizations: Option[Seq[Rule[TypedPipe]]]): Option[Execution[Unit]] = {
    val rfd = new RichFlowDef(fd)
    // TypedPipe jobs should not have modified
    // the FlowDef yet, only the FlowState should
    // be updated
    if (rfd.isEmpty) {
      FlowStateMap.get(fd).flatMap { // Note, we want this to be a pure function so we don't mutate the FlowStateMap
        case FlowState(srcs, confs, writes) if srcs.isEmpty && confs.isEmpty =>
          writes match {
            case Nil => None
            case nonEmpty =>
              partitionOptimizations match {
                case None =>
                  def write[A](w: FlowStateMap.TypedWrite[A]): Execution[Unit] =
                    w.pipe.writeExecution(w.sink)

                  Some(Execution.sequence(nonEmpty.map(write(_))).unit)
                case Some(rules) =>
                  def toPair[A](f: FlowStateMap.TypedWrite[A]): WritePartitioner.PairK[TypedPipe, TypedSink, A] =
                    (f.pipe, f.sink)

                  val pairs: List[WritePartitioner.PairK[TypedPipe, TypedSink, _]] = nonEmpty.map(toPair(_))
                  Some(WritePartitioner.materialize[Execution](rules, pairs))
              }
          }
        case fs =>
          // we can't handle if there have been anything other than TypedPipe.write on the
          // TypedPipe
          Some(Execution.failed(new Exception(s"expected empty FlowState other than TypedWrites, found: $fs")))
      }
    }
    else Some(Execution.failed(new Exception(s"We can only convert Typed-API Jobs to Execution. Found non-empty FlowDef: $fd")))
  }

  /**
   * This converts the TypedPipe to a cascading Pipe doing the most direct
   * possible translation we can. This is useful for testing or for expert
   * cases where you want more direct control of the TypedPipe than
   * the default method gives you.
   */
  final def toPipeUnoptimized[U](input: TypedPipe[U],
    fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {

    val compiler = cache.get(flowDef, mode)

    /**
     * These rules are not optimizations, but actually required for Cascading to not
     * throw. Cascading requires certain shapes of the graphs
     */
    val p = OptimizationRules(input,
      OptimizationRules.DescribeLater
        .orElse(OptimizationRules.DeferMerge)
        .orElse(OptimizationRules.DiamondToFlatMap))

    val cp: CascadingPipe[U] = compiler(p)

    RichPipe(cp.toPipe(fieldNames, flowDef, TupleSetter.asSubSetter(setter)))
      // TODO: this indirection may not be needed anymore, we could directly track config changes
      // rather than using FlowStateMap. This is the only call of this method, so maybe we can
      // remove it.
      .applyFlowConfigProperties(flowDef)
  }

  private def planCoGroup[K, R](cg: CoGrouped[K, R], rec: FunctionK[TypedPipe, CascadingPipe]): CascadingPipe[(K, R)] = {

    // This has the side effect of planning all inputs now
    // before we need to call them below
    val inputsCR = cg.inputs.map(rec(_))

    import cg.{inputs, joinFunction}
    // Cascading handles the first item in join differently, we have to see if it is repeated
    val firstCount = inputs.count(_ == inputs.head)

    import Dsl._
    import RichPipe.assignName

    val flowDef = new FlowDef

    def toPipe[A, B](t: TypedPipe[(A, B)], f: Fields, setter: TupleSetter[(A, B)]): Pipe =
      rec(t).toPipe(f, flowDef, TupleSetter.asSubSetter(setter))
    /*
     * we only want key and value.
     * Cascading requires you have the same number coming in as out.
     * in the first case, we introduce (null0, null1), in the second
     * we have (key1, value1), but they are then discarded:
     */
    def outFields(inCount: Int): Fields =
      List("key", "value") ++ (0 until (2 * (inCount - 1))).map("null%d".format(_))

    // Make this stable so the compiler does not make a closure
    val ord = cg.keyOrdering

    val newPipe = maybeBox[K, Any](ord, flowDef) { (tupset, ordKeyField) =>
      if (firstCount == inputs.size) {
        /**
         * This is a self-join
         * Cascading handles this by sending the data only once, spilling to disk if
         * the groups don't fit in RAM, then doing the join on this one set of data.
         * This is fundamentally different than the case where the first item is
         * not repeated. That case is below
         */
        val NUM_OF_SELF_JOINS = firstCount - 1
        new CoGroup(assignName(toPipe[K, Any](inputs.head, kvFields, tupset)),
          ordKeyField,
          NUM_OF_SELF_JOINS,
          outFields(firstCount),
          WrappedJoiner(new DistinctCoGroupJoiner(firstCount, keyGetter(ord), joinFunction)))
      } else if (firstCount == 1) {

        def keyId(idx: Int): String = "key%d".format(idx)
        /**
         * As long as the first one appears only once, we can handle self joins on the others:
         * Cascading does this by maybe spilling all the streams other than the first item.
         * This is handled by a different CoGroup constructor than the above case.
         */
        def renamePipe(idx: Int, p: TypedPipe[(K, Any)]): Pipe =
          toPipe[K, Any](p, List(keyId(idx), "value%d".format(idx)), tupset)

        // This is tested for the properties we need (non-reordering)
        val distincts = CoGrouped.distinctBy(inputs)(identity)
        val dsize = distincts.size
        val isize = inputs.size

        def makeFields(id: Int): Fields = {
          val comp = ordKeyField.getComparators.apply(0)
          val fieldName = keyId(id)
          val f = new Fields(fieldName)
          f.setComparator(fieldName, comp)
          f
        }

        val groupFields: Array[Fields] = (0 until dsize)
          .map(makeFields)
          .toArray

        val pipes: Array[Pipe] = distincts
          .zipWithIndex
          .map { case (item, idx) => assignName(renamePipe(idx, item)) }
          .toArray

        val cjoiner = if (isize != dsize) {
          // avoid capturing anything other than the mapping ints:
          val mapping: Map[Int, Int] = inputs.zipWithIndex.map {
            case (item, idx) =>
              idx -> distincts.indexWhere(_ == item)
          }.toMap

          new CoGroupedJoiner(isize, keyGetter(ord), joinFunction) {
            val distinctSize = dsize
            def distinctIndexOf(orig: Int) = mapping(orig)
          }
        } else {
          new DistinctCoGroupJoiner(isize, keyGetter(ord), joinFunction)
        }

        new CoGroup(pipes, groupFields, outFields(dsize), WrappedJoiner(cjoiner))
      } else {
        /**
         * This is non-trivial to encode in the type system, so we throw this exception
         * at the planning phase.
         */
        sys.error("Except for self joins, where you are joining something with only itself,\n" +
          "left-most pipe can only appear once. Firsts: " +
          inputs.collect { case x if x == inputs.head => x }.toString)
      }
    }
    /*
       * the CoGrouped only populates the first two fields, the second two
       * are null. We then project out at the end of the method.
       */
    val pipeWithRedAndDescriptions = {
      RichPipe.setReducers(newPipe, cg.reducers.getOrElse(-1))
      RichPipe.setPipeDescriptions(newPipe, cg.descriptions)
      newPipe.project(kvFields)
    }

    CascadingPipe[(K, R)](
      pipeWithRedAndDescriptions,
      kvFields,
      flowDef,
      tuple2Converter[K, R])
  }

  /**
   * TODO: most of the complexity of this method should be rewritten
   * as an optimization rule that works on the scalding typed AST.
   * the code in here gets pretty complex and depending on the details
   * of cascading and also how we compile to cascading.
   *
   * But the optimization is somewhat general: we often want a checkpoint
   * before a hashjoin is replicated
   */
  private def planHashJoin[K, V1, V2, R](left: TypedPipe[(K, V1)],
    right: HashJoinable[K, V2],
    joiner: (K, V1, Iterable[V2]) => Iterator[R],
    rec: FunctionK[TypedPipe, CascadingPipe]): CascadingPipe[(K, R)] = {

    val fd = new FlowDef
    val leftPipe = rec(left).toPipe(kvFields, fd, tup2Setter)
    val mappedPipe = rec(right.mapped).toPipe(new Fields("key1", "value1"), fd, tup2Setter)

    val singleValuePerRightKey = CoGroupable.atMostOneValue(right)
    val keyOrdering = right.keyOrdering
    val hashPipe = new HashJoin(
      RichPipe.assignName(leftPipe),
      Field.singleOrdered("key")(keyOrdering),
      mappedPipe,
      Field.singleOrdered("key1")(keyOrdering),
      WrappedJoiner(new HashJoiner(singleValuePerRightKey, right.joinFunction, joiner)))

    CascadingPipe[(K, R)](
      RichPipe(hashPipe).project(kvFields),
      kvFields,
      fd,
      tuple2Converter[K, R])
  }

  private def planReduceStep[K, V1, V2](
    rs: ReduceStep[K, V1, V2],
    rec: FunctionK[TypedPipe, CascadingPipe]): CascadingPipe[(K, V2)] = {

    val mapped = rec(rs.mapped)

    def groupOp(gb: GroupBuilder => GroupBuilder): CascadingPipe[_ <: (K, V2)] =
      groupOpWithValueSort(None)(gb)

    def groupOpWithValueSort(valueSort: Option[Ordering[V1]])(gb: GroupBuilder => GroupBuilder): CascadingPipe[_ <: (K, V2)] = {
      val flowDef = new FlowDef
      val pipe = maybeBox[K, V1](rs.keyOrdering, flowDef) { (tupleSetter, fields) =>
        val (sortOpt, ts) = valueSort.map {
          case ordser: OrderedSerialization[V1 @unchecked] =>
            // We get in here when we do a secondary sort
            // and that sort is an ordered serialization
            // We now need a boxed serializer for this type
            // Then we set the comparator on the field, and finally we box the value with our tupleSetter
            val (boxfn, boxordSer) = getBoxFnAndOrder[V1](ordser, flowDef)
            val valueF = new Fields("value")
            valueF.setComparator("value", new CascadingBinaryComparator(boxordSer))
            val ts2 = tupleSetter.asInstanceOf[TupleSetter[(K, Boxed[V1])]].contraMap { kv1: (K, V1) => (kv1._1, boxfn(kv1._2)) }
            (Some(valueF), ts2)
          case vs =>
            val vord = Field.singleOrdered("value")(vs)
            (Some(vord), tupleSetter)
        }.getOrElse((None, tupleSetter))

        val p = mapped.toPipe(kvFields, flowDef, TupleSetter.asSubSetter(ts))

        RichPipe(p).groupBy(fields) { inGb =>
          val withSort = sortOpt.fold(inGb)(inGb.sortBy)
          gb(withSort)
        }
      }

      val tupConv = tuple2Conv[K, V2](rs.keyOrdering)
      CascadingPipe(pipe, kvFields, flowDef, tupConv)
    }

    rs match {
      case ir@IdentityReduce(_, _, None, descriptions, _) =>
        type CP[V] = CascadingPipe[_ <: (K, V)]
        // Not doing anything
        ir.evidence.subst[CP](mapped.copy(pipe = RichPipe.setPipeDescriptions(mapped.pipe, descriptions)))
      case uir@UnsortedIdentityReduce(_, _, None, descriptions, _) =>
        type CP[V] = CascadingPipe[_ <: (K, V)]
        // Not doing anything
        uir.evidence.subst[CP](mapped.copy(pipe = RichPipe.setPipeDescriptions(mapped.pipe, descriptions)))
      case IdentityReduce(_, _, Some(reds), descriptions, _) =>
        groupOp { _.reducers(reds).setDescriptions(descriptions) }
      case UnsortedIdentityReduce(_, _, Some(reds), descriptions, _) =>
        // This is weird, but it is sometimes used to force a partition
        groupOp { _.reducers(reds).setDescriptions(descriptions) }
      case ivsr@IdentityValueSortedReduce(_, _, _, _, _, _) =>
        groupOpWithValueSort(Some(ivsr.valueSort)) { gb =>
          // If its an ordered serialization we need to unbox
          val mappedGB =
            if (ivsr.valueSort.isInstanceOf[OrderedSerialization[_]])
              gb.mapStream[Boxed[V1], V1](valueField -> valueField) { it: Iterator[Boxed[V1]] =>
                it.map(_.get)
              }
            else
              gb

          mappedGB
            .reducers(ivsr.reducers.getOrElse(-1))
            .setDescriptions(ivsr.descriptions)
        }
      case vsr@ValueSortedReduce(_, _, _, _, _, _) =>
        val optVOrdering = Some(vsr.valueSort)
        groupOpWithValueSort(optVOrdering) {
          // If its an ordered serialization we need to unbox
          // the value before handing it to the users operation
          _.every(new cascading.pipe.Every(_, valueField,
            new TypedBufferOp[K, V1, V2](keyConverter(vsr.keyOrdering),
              valueConverter(optVOrdering),
              vsr.reduceFn,
              valueField), Fields.REPLACE))
            .reducers(vsr.reducers.getOrElse(-1))
            .setDescriptions(vsr.descriptions)
        }
      case imr@IteratorMappedReduce(_, _, _, _, _) =>
        groupOp {
          _.every(new cascading.pipe.Every(_, valueField,
            new TypedBufferOp(keyConverter(imr.keyOrdering), TupleConverter.singleConverter[V1], imr.reduceFn, valueField), Fields.REPLACE))
            .reducers(imr.reducers.getOrElse(-1))
            .setDescriptions(imr.descriptions)
        }
    }
  }
}

