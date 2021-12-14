package com.twitter.scalding.beam_backend

import com.stripe.dagon.{FunctionK, Memoize, Rule}
import com.twitter.chill.KryoInstantiator
import com.twitter.chill.config.ScalaMapConfig
import com.twitter.scalding.Config
import com.twitter.scalding.beam_backend.BeamOp.{CoGroupedOp, MergedBeamOp}
import com.twitter.scalding.serialization.KryoHadoop
import com.twitter.scalding.typed._
import com.twitter.scalding.typed.functions.{
  FilterKeysToFilter,
  FlatMapValuesToFlatMap,
  MapValuesToMap,
  ScaldingPriorityQueueMonoid
}

object BeamPlanner {
  def plan(
      config: Config,
      srcs: Resolver[TypedSource, BeamSource]
  ): FunctionK[TypedPipe, BeamOp] = {
    implicit val kryoCoder: KryoCoder = new KryoCoder(defaultKryoCoderConfiguration(config))
    Memoize.functionK(f = new Memoize.RecursiveK[TypedPipe, BeamOp] {

      import TypedPipe._

      def toFunction[A] = {
        case (f @ Filter(_, _), rec) =>
          def go[T](f: Filter[T]): BeamOp[T] = {
            val Filter(p, fn) = f
            rec[T](p).filter(fn)
          }
          go(f)
        case (fk @ FilterKeys(_, _), rec) =>
          def go[K, V](node: FilterKeys[K, V]): BeamOp[(K, V)] = {
            val FilterKeys(pipe, fn) = node
            rec(pipe).filter(FilterKeysToFilter(fn))
          }
          go(fk)
        case (Mapped(input, fn), rec) =>
          val op = rec(input)
          op.map(fn)
        case (FlatMapped(input, fn), rec) =>
          val op = rec(input)
          op.flatMap(fn)
        case (f @ MapValues(_, _), rec) =>
          def go[K, V, U](node: MapValues[K, V, U]): BeamOp[(K, U)] = {
            val MapValues(pipe, fn) = node
            rec(pipe).map(MapValuesToMap[K, V, U](fn))
          }
          go(f)
        case (f @ FlatMapValues(_, _), rec) =>
          def go[K, V, U](node: FlatMapValues[K, V, U]): BeamOp[(K, U)] = {
            val FlatMapValues(pipe, fn) = node
            rec(pipe).flatMap(FlatMapValuesToFlatMap[K, V, U](fn))
          }
          go(f)
        case (SourcePipe(src), _) =>
          BeamOp.Source(config, src, srcs(src))
        case (IterablePipe(iterable), _) =>
          BeamOp.FromIterable(iterable, kryoCoder)
        case (wd: WithDescriptionTypedPipe[a], rec) =>
          rec[a](wd.input)
        case (SumByLocalKeys(pipe, sg), rec) =>
          val op = rec(pipe)
          config.getMapSideAggregationThreshold match {
            case None        => op
            case Some(count) =>
              // Semigroup is invariant on T. We cannot pattern match as it is a Semigroup[PriorityQueue[T]]
              if (sg.isInstanceOf[ScaldingPriorityQueueMonoid[_]]) {
                op
              } else {
                op.mapSideAggregator(count, sg)
              }
          }
        case (ReduceStepPipe(ir @ IdentityReduce(_, _, _, _, _)), rec) =>
          def go[K, V1, V2](ir: IdentityReduce[K, V1, V2]): BeamOp[(K, V2)] = {
            type BeamOpT[V] = BeamOp[(K, V)]
            val op = rec(ir.mapped)
            ir.evidence.subst[BeamOpT](op)
          }
          go(ir)
        case (ReduceStepPipe(uir @ UnsortedIdentityReduce(_, _, _, _, _)), rec) =>
          def go[K, V1, V2](uir: UnsortedIdentityReduce[K, V1, V2]): BeamOp[(K, V2)] = {
            type BeamOpT[V] = BeamOp[(K, V)]
            val op = rec(uir.mapped)
            uir.evidence.subst[BeamOpT](op)
          }
          go(uir)
        case (ReduceStepPipe(ivsr @ IdentityValueSortedReduce(_, _, _, _, _, _)), rec) =>
          def go[K, V1, V2](uir: IdentityValueSortedReduce[K, V1, V2]): BeamOp[(K, V2)] = {
            type BeamOpT[V] = BeamOp[(K, V)]
            val op = rec(uir.mapped)
            val sortedOp = op.sorted(uir.keyOrdering, uir.valueSort, kryoCoder)
            uir.evidence.subst[BeamOpT](sortedOp)
          }
          go(ivsr)
        case (ReduceStepPipe(ValueSortedReduce(keyOrdering, pipe, valueSort, reduceFn, _, _)), rec) =>
          val op = rec(pipe)
          op.sortedMapGroup(reduceFn)(keyOrdering, valueSort, kryoCoder)
        case (ReduceStepPipe(IteratorMappedReduce(keyOrdering, pipe, reduceFn, _, _)), rec) =>
          val op = rec(pipe)
          op.mapGroup(reduceFn)(keyOrdering, kryoCoder)
        case (hcg @ HashCoGroup(_, _, _), rec) =>
          def go[K, V1, V2, W](hcg: HashCoGroup[K, V1, V2, W]): BeamOp[(K, W)] = {
            val leftOp = rec(hcg.left)
            implicit val orderingK: Ordering[K] = hcg.right.keyOrdering
            val rightOp = rec(ReduceStepPipe(HashJoinable.toReduceStep(hcg.right)))
            leftOp.hashJoin(rightOp, hcg.joiner)
          }
          go(hcg)
        case (CoGroupedPipe(cg), rec) =>
          def go[K, V](cg: CoGrouped[K, V]): BeamOp[(K, V)] = {
            val ops: Seq[BeamOp[(K, Any)]] = cg.inputs.map(tp => rec(tp))
            CoGroupedOp(cg, ops)
          }
          go(cg)
        case (Fork(input), rec) =>
          rec(input)
        case (m @ MergedTypedPipe(_, _), rec) =>
          OptimizationRules.unrollMerge(m) match {
            case Nil        => rec(EmptyTypedPipe)
            case one :: Nil => rec(one)
            case many       => MergedBeamOp(many.map(rec(_)))
          }
      }
    })
  }

  def defaultKryoCoderConfiguration(config: Config): KryoInstantiator =
    config.getKryo match {
      case Some(kryoInstantiator) => kryoInstantiator
      case None                   => new KryoHadoop(new ScalaMapConfig(Map.empty))
    }

  def defaultOptimizationRules(config: Config): Seq[Rule[TypedPipe]] = {
    def std(forceHash: Rule[TypedPipe]) =
      OptimizationRules.standardMapReduceRules :::
        List(
          OptimizationRules.FilterLocally, // after filtering, we may have filtered to nothing, lets see
          OptimizationRules.simplifyEmpty,
          // add any explicit forces to the optimized graph
          Rule.orElse(List(forceHash, OptimizationRules.RemoveDuplicateForceFork))
        )

    config.getOptimizationPhases match {
      case Some(tryPhases) => tryPhases.get.phases
      case None =>
        val force =
          if (config.getHashJoinAutoForceRight) OptimizationRules.ForceToDiskBeforeHashJoin
          else Rule.empty[TypedPipe]
        std(force)
    }
  }
}
