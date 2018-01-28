package com.twitter.scalding.typed.cascading_backend


import cascading.flow.FlowDef
import cascading.operation.{ Debug, Operation }
import cascading.pipe.{ CoGroup, Each, Pipe, HashJoin }
import cascading.tuple.{ Fields, Tuple => CTuple, TupleEntry }
import com.stripe.dagon.{ FunctionK, HCache, Memoize, Rule, Dag }
import com.twitter.scalding.TupleConverter.{ singleConverter, tuple2Converter }
import com.twitter.scalding.TupleSetter.{ singleSetter, tup2Setter }
import com.twitter.scalding.{
  CleanupIdentityFunction, Config, Dsl, Field, FlatMapFunction, FlowStateMap, GroupBuilder,
  HadoopMode, LineNumber, IterableSource, MapsideReduce, Mode, RichFlowDef,
  RichPipe, TupleConverter, TupleGetter, TupleSetter, TypedBufferOp, WrappedJoiner, Write
}
import com.twitter.scalding.typed._
import com.twitter.scalding.typed.functions.{ FilterKeysToFilter, MapValuesToMap, FlatMapValuesToFlatMap, FlatMappedFn }
import com.twitter.scalding.serialization.{
  Boxed,
  BoxedOrderedSerialization,
  CascadingBinaryComparator,
  EquivSerialization,
  OrderedSerialization,
  WrappedSerialization
}
import java.util.WeakHashMap
import scala.collection.mutable.{ Map => MMap }
import org.apache.hadoop.mapred.JobConf
import com.twitter.scalding.quotation.Quoted

object CascadingBackend {
  import TypedPipe._
  
  private implicit val q: Quoted = Quoted.internal

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

  private def valueConverter[V](optOrd: Option[Ordering[_ >: V]]): TupleConverter[V] =
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
          FlowStateMap.mutate(flowDef) { st =>
            val newSt = st.addConfigSetting(k + cls, v)
            (newSt, ())
          }
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

   private case class CascadingPipe[+T](pipe: Pipe,
    fields: Fields,
    @transient localFlowDef: FlowDef, // not serializable.
    converter: TupleConverter[_ <: T]) {

    /**
     * merge the flowDef into this new flowdef an make sure the tuples
     * have the structure defined by setter
     */
    def toPipe[U >: T](f: Fields, fd: FlowDef, setter: TupleSetter[U]): Pipe = {
      // TODO, this may be identity if the setter is the inverse of the
      // converter. If we can identify this we will save allocations
      val resFd = new RichFlowDef(fd)
      resFd.mergeFrom(localFlowDef)
      RichPipe(pipe).mapTo[T, U](fields -> f)(t => t)(TupleConverter.asSuperConverter(converter), setter)
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

  private def compile[T](mode: Mode): FunctionK[TypedPipe, CascadingPipe] =
    Memoize.functionK[TypedPipe, CascadingPipe](
      new Memoize.RecursiveK[TypedPipe, CascadingPipe] {
        def toFunction[T] = {
          case (cp@CrossPipe(_, _), rec) =>
            rec(cp.viaHashJoin)
          case (cv@CrossValue(_, _), rec) =>
            rec(cv.viaHashJoin)
          case (DebugPipe(p), rec) =>
            val inner = rec(p)
            inner.copy(pipe = new Each(inner.pipe, new Debug))
          case (EmptyTypedPipe, rec) =>
            // just use an empty iterable pipe.
            rec(IterablePipe(List.empty[T]))
          case (fk@FilterKeys(_, _), rec) =>
            def go[K, V](node: FilterKeys[K, V]): CascadingPipe[(K, V)] = {
              val rewrite = Filter[(K, V)](node.input, FilterKeysToFilter(node.fn))
              rec(rewrite)
            }
            go(fk)
          case (f@Filter(_, _), rec) =>
            // hand holding for type inference
            def go[T1 <: T](f: Filter[T1]): CascadingPipe[T] =
              // TODO, maybe it is better to tell cascading we are
              // doing a filter here: https://github.com/cwensel/cascading/blob/wip-4.0/cascading-core/src/main/java/cascading/operation/Filter.java
              rec(FlatMapped(f.input, FlatMappedFn.fromFilter(f.fn)))

            go(f)
          case (f@FlatMapValues(_, _), rec) =>
            def go[K, V, U](node: FlatMapValues[K, V, U]): CascadingPipe[T] =
              rec(FlatMapped[(K, V), (K, U)](node.input, FlatMapValuesToFlatMap(node.fn)))

            go(f)
          case (fm@FlatMapped(_, _), rec) =>
            // TODO we can optimize a flatmapped input directly and skip some tupleconverters
            def go[A, B <: T](fm: FlatMapped[A, B]): CascadingPipe[T] = {
              val CascadingPipe(pipe, initF, fd, conv) = rec(fm.input)
              val fmpipe = RichPipe(pipe).flatMapTo[A, T](initF -> f0)(fm.fn)(TupleConverter.asSuperConverter(conv), singleSetter)
              CascadingPipe.single[B](fmpipe, fd)
            }

            go(fm)
          case (ForceToDisk(input), rec) =>
            val cp = rec(input)
            cp.copy(pipe = RichPipe(cp.pipe).forceToDisk)
          case (Fork(input), rec) =>
            // fork doesn't mean anything here since we are already planning each TypedPipe to
            // something in cascading. Fork is an optimizer level operation
            rec(input)
          case (IterablePipe(iter), _) =>
            val fd = new FlowDef
            val pipe = IterableSource[T](iter, f0)(singleSetter, singleConverter).read(fd, mode)
            CascadingPipe.single[T](pipe, fd)
          case (f@MapValues(_, _), rec) =>
            def go[K, A, B](fn: MapValues[K, A, B]): CascadingPipe[_ <: (K, B)] =
              rec(Mapped[(K, A), (K, B)](fn.input, MapValuesToMap(fn.fn)))

            go(f)
          case (Mapped(input, fn), rec) =>
            // TODO: a native scalding Map operation would be slightly more efficient here
            rec(FlatMapped(input, FlatMappedFn.fromMap(fn)))

          case (m@MergedTypedPipe(_, _), rec) =>
            OptimizationRules.unrollMerge(m) match {
              case Nil => rec(EmptyTypedPipe)
              case h :: Nil => rec(h)
              case nonEmpty =>
                // TODO: a better optimization is to not materialize this
                // node at all if there is no fan out since groupBy and cogroupby
                // can accept multiple inputs
                //
                // (a ++ a) == a.flatMap { x => List(x, x) } is an optimization we used to
                // have

                val flowDef = new FlowDef
                // if all of the converters are the same, we could skip some work
                // here, but need to be able to see that correctly
                val pipes = nonEmpty.map { p => rec(p).toPipe(f0, flowDef, singleSetter) }
                val merged = new cascading.pipe.Merge(pipes.map(RichPipe.assignName): _*)
                CascadingPipe.single[T](merged, flowDef)
            }
          case (SourcePipe(typedSrc, p), _) =>
            val fd = new FlowDef
            val pipe = typedSrc.read(fd, mode)
            typedSrc.projectionMeta.foreach { meta =>
              (p, mode) match {
                case (Some(p), h: HadoopMode) => 
                  meta.setProjections(h.jobConf, p)
                case _ => 
              }
            }
            CascadingPipe[T](pipe, typedSrc.sourceFields, fd, typedSrc.converter[T])
          case (sblk@SumByLocalKeys(_, _), rec) =>
            def go[K, V](sblk: SumByLocalKeys[K, V]): CascadingPipe[(K, V)] = {
              val cp = rec(sblk.input)
              val localFD = new FlowDef
              val cpKV: Pipe = cp.toPipe(kvFields, localFD, tup2Setter)
              val msr = new MapsideReduce(sblk.semigroup, new Fields("key"), valueField, None)(singleConverter[V], singleSetter[V])
              val kvpipe = RichPipe(cpKV).eachTo(kvFields -> kvFields) { _ => msr }
              CascadingPipe(kvpipe, kvFields, localFD, tuple2Converter[K, V])
            }
            go(sblk)
          case (trapped: TrappedPipe[u], rec) =>
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
          case (WithDescriptionTypedPipe(input, descr, dedup, quoted), rec) =>

            @annotation.tailrec
            def loop[A](t: TypedPipe[A], acc: List[(String, Boolean)]): (TypedPipe[A], List[(String, Boolean)]) =
              t match {
                case WithDescriptionTypedPipe(i, desc, ded, quoted) =>
                  loop(i, (desc, ded) :: acc)
                case notDescr => (notDescr, acc)
              }

            val (root, descrs) = loop(input, (descr, dedup) :: Nil)
            val cp = rec(root)
            cp.copy(pipe = applyDescriptions(cp.pipe, descrs))

          case (WithOnComplete(input, fn), rec) =>
            val cp = rec(input)
            val next = new Each(cp.pipe, Fields.ALL, new CleanupIdentityFunction(fn), Fields.REPLACE)
            cp.copy(pipe = next)

          case (hcg@HashCoGroup(_, _, _), rec) =>
            def go[K, V1, V2, R](hcg: HashCoGroup[K, V1, V2, R]): CascadingPipe[(K, R)] =
              planHashJoin(hcg.left,
                hcg.right,
                hcg.joiner,
                rec)

            go(hcg)
          case (ReduceStepPipe(rs), rec) =>
            planReduceStep(rs, rec)

          case (CoGroupedPipe(cg), rec) =>
            planCoGroup(cg, rec)
        }
      })

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
    /**
     * TODO:
     * we need to have parity for the normal optimizations
     * scalding has been applying in 0.17
     *
     */
    def std(forceHash: Rule[TypedPipe], automaticProjectionPushdown: Rule[TypedPipe]) =
      OptimizationRules.standardMapReduceRules :+
        // add any explicit forces to the optimized graph
        Rule.orElse(List(
          forceHash,
          OptimizationRules.RemoveDuplicateForceFork)) :+
        automaticProjectionPushdown

    config.getOptimizationPhases match {
      case Some(tryPhases) => tryPhases.get.phases
      case None =>

        def onlyIf(cond: Boolean)(rule: Rule[TypedPipe]): Rule[TypedPipe] =
          if (cond) rule
          else Rule.empty

        val force =
          onlyIf(config.getHashJoinAutoForceRight)(OptimizationRules.ForceToDiskBeforeHashJoin)

        val automaticProjectionPushdown =
          onlyIf(config.getAutomaticProjectionPushdown)(OptimizationRules.ApplyProjectionPushdown)

        std(force, automaticProjectionPushdown)
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
   * This converts the TypedPipe to a cascading Pipe doing the most direct
   * possible translation we can. This is useful for testing or for expert
   * cases where you want more direct control of the TypedPipe than
   * the default method gives you.
   */
  final def toPipeUnoptimized[U](p: TypedPipe[U],
    fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {

    val compiler = cache.get(flowDef, mode)
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

    val keyOrdering = right.keyOrdering
    val hashPipe = new HashJoin(
      RichPipe.assignName(leftPipe),
      Field.singleOrdered("key")(keyOrdering),
      mappedPipe,
      Field.singleOrdered("key1")(keyOrdering),
      WrappedJoiner(new HashJoiner(right.joinFunction, joiner)))

    CascadingPipe[(K, R)](
      hashPipe,
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

    def groupOpWithValueSort(valueSort: Option[Ordering[_ >: V1]])(gb: GroupBuilder => GroupBuilder): CascadingPipe[_ <: (K, V2)] = {
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
      case IdentityReduce(_, _, None, descriptions) =>
        // Not doing anything
        mapped.copy(pipe = RichPipe.setPipeDescriptions(mapped.pipe, descriptions)).asInstanceOf[CascadingPipe[_ <: (K, V2)]]
      case UnsortedIdentityReduce(_, _, None, descriptions) =>
        // Not doing anything
        mapped.copy(pipe = RichPipe.setPipeDescriptions(mapped.pipe, descriptions)).asInstanceOf[CascadingPipe[_ <: (K, V2)]]
      case IdentityReduce(_, _, Some(reds), descriptions) =>
        groupOp { _.reducers(reds).setDescriptions(descriptions) }
      case UnsortedIdentityReduce(_, _, Some(reds), descriptions) =>
        // This is weird, but it is sometimes used to force a partition
        groupOp { _.reducers(reds).setDescriptions(descriptions) }
      case ivsr@IdentityValueSortedReduce(_, _, _, _, _) =>
        // in this case we know that V1 =:= V2
        groupOpWithValueSort(Some(ivsr.valueSort.asInstanceOf[Ordering[_ >: V1]])) { gb =>
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

