package com.twitter.scalding.typed

import com.twitter.scalding.Execution
import com.stripe.dagon.{ Dag, Id, Rule, Memoize, FunctionK }
import org.slf4j.LoggerFactory
import scala.language.higherKinds

object WritePartitioner {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  type PairK[F[_], G[_], T] = (F[T], G[T])

  /**
   * This breaks a job at all the places it explicitly fans out,
   * (and currently after each reduce/join).
   */
  def breakAtForks[M[+_]](ws: List[PairK[TypedPipe, TypedSink, _]])(implicit M: Materializer[M]): M[Unit] = {
    val rules = List(
      OptimizationRules.AddExplicitForks,
      OptimizationRules.RemoveDuplicateForceFork)
    materialize[M](rules, ws)
  }

  /**
   * This enables us to write the partitioning in terms of this
   * applicative type that is equiped with two extra operations:
   * materialized and write, but not a general flatMap
   *
   * so the only sequencing power we have is to materialize
   *
   * This allows us to test the properties we want without
   * having to deal with Execution, which is a black box
   * concerned with actually running jobs
   */
  trait Materializer[M[+_]] {
    type TP[+A] = M[TypedPipe[A]]

    def pure[A](a: A): M[A]
    def map[A, B](ma: M[A])(fn: A => B): M[B]
    def zip[A, B](ma: M[A], mb: M[B]): M[(A, B)]
    def materialize[A](t: M[TypedPipe[A]]): M[TypedPipe[A]]
    def write[A](tp: M[TypedPipe[A]], sink: TypedSink[A]): M[Unit]
    def sequence_[A](as: Seq[M[A]]): M[Unit]
  }

  object Materializer {
    implicit val executionMaterializer: Materializer[Execution] =
      new Materializer[Execution] {
        def pure[A](a: A) = Execution.from(a)
        def map[A, B](ma: Execution[A])(fn: A => B) = ma.map(fn)
        def zip[A, B](ma: Execution[A], mb: Execution[B]): Execution[(A, B)] = ma.zip(mb)
        def materialize[A](t: Execution[TypedPipe[A]]): Execution[TypedPipe[A]] = t.flatMap(_.forceToDiskExecution)
        def write[A](tp: Execution[TypedPipe[A]], sink: TypedSink[A]): Execution[Unit] =
          tp.flatMap(_.writeExecution(sink))
        def sequence_[A](as: Seq[Execution[A]]): Execution[Unit] = Execution.sequence(as).unit
      }
  }

  def materialize[M[+_]](phases: Seq[Rule[TypedPipe]], ws: List[PairK[TypedPipe, TypedSink, _]])(implicit mat: Materializer[M]): M[Unit] = {
    val writes = materialize1[M, TypedSink](phases, ws)(mat)
    val toSeq = writes.map { case (mt, sink) => mat.write(mt, sink) }
    mat.sequence_(toSeq)
  }

  def materialize1[M[+_], S[_]](phases: Seq[Rule[TypedPipe]],
    ws: List[PairK[TypedPipe, S, _]])(implicit mat: Materializer[M]): List[PairK[mat.TP, S, _]] = {
    val e = Dag.empty(OptimizationRules.toLiteral)

    logger.info(s"converting ${ws.size} writes into several parts")
    val (finalDag, writeIds) = ws.foldLeft((e, List.empty[PairK[Id, S, _]])) {
      case ((dag, writes), pair) =>
        val (dag1, id) = dag.addRoot(pair._1)
        (dag1, (id, pair._2) :: writes)
    }
    // Now apply the rules:
    logger.info(s"applying rules to graph of size: ${finalDag.allNodes.size}")
    val optDag = finalDag.applySeq(phases :+ OptimizationRules.RemoveUselessFork)
    logger.info(s"optimized graph hash size: ${optDag.allNodes.size}")

    import TypedPipe.{ReduceStepPipe, HashCoGroup}

    def handleHashCoGroup[K, V, V2, R](hj: HashCoGroup[K, V, V2, R], recurse: FunctionK[TypedPipe, mat.TP]): mat.TP[(K, R)] = {
      import TypedPipe._
      val exright: M[HashJoinable[K, V2]] = hj.right match {
        case step@IdentityReduce(_, _, _, _, _) =>
          type TK[+Z] = TypedPipe[(K, Z)]
          val mappedV2 = step.evidence.subst[TK](step.mapped)
          mat.map(recurse(mappedV2)) { (tp: TypedPipe[(K, V2)]) =>
            IdentityReduce[K, V2, V2](step.keyOrdering, tp, step.reducers, step.descriptions, implicitly)
          }
        case step@UnsortedIdentityReduce(_, _, _, _, _) =>
          type TK[+Z] = TypedPipe[(K, Z)]
          val mappedV2 = step.evidence.subst[TK](step.mapped)
          mat.map(recurse(mappedV2)) { (tp: TypedPipe[(K, V2)]) =>
            UnsortedIdentityReduce[K, V2, V2](step.keyOrdering, tp, step.reducers, step.descriptions, implicitly)
          }
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          def go[A, B, C](imr: IteratorMappedReduce[A, B, C]) =
            mat.map(recurse(imr.mapped)) { (tp: TypedPipe[(A, B)]) => imr.copy(mapped = tp) }

          go(step)
      }

      val zipped = mat.zip(recurse(hj.left), exright)
      mat.map(zipped) { case (left, right) =>
        HashCoGroup(left, right, hj.joiner)
      }
    }

    def widen[A, B <: A](exb: M[B]): M[A] = exb

    def handleReduceStep[K, V1, V2](rs: ReduceStep[K, V1, V2], recurse: FunctionK[TypedPipe, mat.TP]): mat.TP[(K, V2)] =
      mat.map(recurse(rs.mapped)) { pipe => TypedPipe.ReduceStepPipe(ReduceStep.setInput[K, V1, V2](rs, pipe)) }

    def handleCoGrouped[K, V](cg: CoGroupable[K, V], recurse: FunctionK[TypedPipe, mat.TP]): mat.TP[(K, V)] = {
      import CoGrouped._
      import TypedPipe._

      def pipeToCG[V1](t: TypedPipe[(K, V1)]): CoGroupable[K, V1] =
        t match {
          case ReduceStepPipe(cg: CoGroupable[K @unchecked, V1 @unchecked]) =>
            // we are relying on the fact that we use Ordering[K]
            // as a contravariant type, despite it not being defined
            // that way.
            cg
          case CoGroupedPipe(cg) =>
            // we are relying on the fact that we use Ordering[K]
            // as a contravariant type, despite it not being defined
            // that way.
            cg.asInstanceOf[CoGroupable[K, V1]]
          case kvPipe => IdentityReduce[K, V1, V1](cg.keyOrdering, kvPipe, None, Nil, implicitly)
        }

      cg match {
        case p@Pair(_, _, _) =>
          def go[A, B, C](pair: Pair[K, A, B, C]): mat.TP[(K, C)] = {
            val mleft = handleCoGrouped(pair.larger, recurse)
            val mright = handleCoGrouped(pair.smaller, recurse)
            val both = mat.zip(mleft, mright)
            mat.map(both) { case (l, r) =>
              CoGroupedPipe(Pair(pipeToCG(l), pipeToCG(r), pair.fn))
            }
          }
          widen(go(p))
        case wr@WithReducers(_, _) =>
          def go[V1 <: V](wr: WithReducers[K, V1]): mat.TP[(K, V)] = {
            val reds = wr.reds
            mat.map(handleCoGrouped(wr.on, recurse)) { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  ReduceStepPipe(ReduceStep.withReducers(rs, reds))
                case CoGroupedPipe(cg) =>
                  CoGroupedPipe(WithReducers(cg, reds))
                case kvPipe =>
                  ReduceStepPipe(IdentityReduce[K, V1, V1](cg.keyOrdering, kvPipe, None, Nil, implicitly)
                    .withReducers(reds))
              }
            }
          }
          go(wr)
        case wd@WithDescription(_, _) =>
          def go[V1 <: V](wd: WithDescription[K, V1]): mat.TP[(K, V)] = {
            val desc = wd.description
            mat.map(handleCoGrouped(wd.on, recurse)) { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  ReduceStepPipe(ReduceStep.withDescription(rs, desc))
                case CoGroupedPipe(cg) =>
                  CoGroupedPipe(WithDescription(cg, desc))
                case kvPipe =>
                  kvPipe.withDescription(desc)
              }
            }
          }
          go(wd)
        case fk@CoGrouped.FilterKeys(_, _) =>
          def go[V1 <: V](fk: CoGrouped.FilterKeys[K, V1]): mat.TP[(K, V)] = {
            val fn = fk.fn
            mat.map(handleCoGrouped(fk.on, recurse)) { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  val mapped = rs.mapped
                  val mappedF = TypedPipe.FilterKeys(mapped, fn)
                  ReduceStepPipe(ReduceStep.setInput(rs, mappedF))
                case CoGroupedPipe(cg) =>
                  CoGroupedPipe(CoGrouped.FilterKeys(cg, fn))
                case kvPipe =>
                  TypedPipe.FilterKeys(kvPipe, fn)
              }
            }
          }
          go(fk)
        case mg@MapGroup(_, _) =>
          def go[V1, V2 <: V](mg: MapGroup[K, V1, V2]): mat.TP[(K, V)] = {
            val fn = mg.fn
            mat.map(handleCoGrouped(mg.on, recurse)) { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  ReduceStepPipe(ReduceStep.mapGroup(rs)(fn))
                case CoGroupedPipe(cg) =>
                  CoGroupedPipe(MapGroup(cg, fn))
                case kvPipe =>
                  val rs = IdentityReduce[K, V1, V1](cg.keyOrdering, kvPipe, None, Nil, implicitly)
                  ReduceStepPipe(ReduceStep.mapGroup(rs)(fn))
              }
            }
          }
          go(mg)
        case step@IdentityReduce(_, _, _, _, _) =>
          widen(handleReduceStep(step, recurse)) // the widen trick sidesteps GADT bugs
        case step@UnsortedIdentityReduce(_, _, _, _, _) =>
          widen(handleReduceStep(step, recurse))
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          widen(handleReduceStep(step, recurse))
      }
    }

    /**
     * If cascading would consider the current pipe as a Logical reduce
     * we can avoid some forces below. This method returns true
     * if the pipe is ending on a reduce (not potentially a map-only job)
     */
    @annotation.tailrec
    def isLogicalReduce(tp: TypedPipe[Any]): Boolean = {
      import TypedPipe._
      tp match {
        case EmptyTypedPipe | IterablePipe(_) | SourcePipe(_) => false
        case CounterPipe(a) => isLogicalReduce(a)
        case cp@CrossPipe(_, _) => isLogicalReduce(cp.viaHashJoin)
        case cp@CrossValue(_, _) => isLogicalReduce(cp.viaHashJoin)
        case DebugPipe(p) => isLogicalReduce(p)
        case FilterKeys(p, _) => isLogicalReduce(p)
        case Filter(p, _) => isLogicalReduce(p)
        case FlatMapValues(p, _) => isLogicalReduce(p)
        case FlatMapped(p, _) => isLogicalReduce(p)
        case ForceToDisk(_) => false // not reducers for sure, could be a map-only job
        case Fork(_) => false // TODO, not super clear
        case HashCoGroup(left, _, _) => isLogicalReduce(left)
        case MapValues(p, _) => isLogicalReduce(p)
        case Mapped(p, _) => isLogicalReduce(p)
        case MergedTypedPipe(_, _) => false
        case ReduceStepPipe(_) => true
        case SumByLocalKeys(p, _) => isLogicalReduce(p)
        case TrappedPipe(p, _, _) => isLogicalReduce(p)
        case CoGroupedPipe(_) => true
        case WithOnComplete(p, _) => isLogicalReduce(p)
        case WithDescriptionTypedPipe(p, _) => isLogicalReduce(p)
      }
    }

    /**
     * We use this state to track where we are as we recurse up the graph.
     * Since we know at the very end we will write, we can avoid, for instance
     * forcing a reduce operation that is followed only by a map and a write.
     *
     * Coupled with the isLogicalReduce above, we can emulate the behavior
     * of the cascading planner as we recurse up.
     */
    sealed abstract class BelowState {
      def |(that: BelowState): BelowState =
        (this, that) match {
          case (BelowState.Write, later) => later
          case (BelowState.OnlyMapping, BelowState.Write) => BelowState.OnlyMapping
          case (BelowState.OnlyMapping, mapOrMater) => mapOrMater
          case (BelowState.Materialized, _) => BelowState.Materialized
        }
    }
    object BelowState {
      case object Write extends BelowState
      case object OnlyMapping extends BelowState
      case object Materialized extends BelowState
    }
    type P[a] = (TypedPipe[a], BelowState)
    /**
     * Given a pipe, and the state below it, return the materialized
     * version of that pipe. This should cause no more materializations
     * than cascading would do, and indeed we test for this property
     */
    val fn = Memoize.functionK[P, mat.TP](
      new Memoize.RecursiveK[P, mat.TP] {
        import TypedPipe._
        import BelowState._

        def toFunction[A] = {
          case ((cp: CounterPipe[a], bs), rec) =>
            mat.map(rec((cp.pipe, bs)))(CounterPipe(_: TypedPipe[(a, Iterable[((String, String), Long)])]))
          case ((c: CrossPipe[a, b], bs), rec) =>
            rec((c.viaHashJoin, bs))
          case ((cv@CrossValue(_, _), bs), rec) =>
            rec((cv.viaHashJoin, bs))
          case ((p: DebugPipe[a], bs), rec) =>
            mat.map(rec((p.input, bs)))(DebugPipe(_: TypedPipe[a]))
          case ((p: FilterKeys[a, b], bs), rec) =>
            mat.map(rec((p.input, bs | OnlyMapping)))(FilterKeys(_: TypedPipe[(a, b)], p.fn))
          case ((p: Filter[a], bs), rec) =>
            mat.map(rec((p.input, bs | OnlyMapping)))(Filter(_: TypedPipe[a], p.fn))
          case ((Fork(of), bs), rec) =>
            // Treat forks as forceToDisk after
            // optimizations (which should have removed unneeded forks
            rec((ForceToDisk(of), bs))
          case ((p: FlatMapValues[a, b, c], bs), rec) =>
            mat.map(rec((p.input, bs | OnlyMapping)))(FlatMapValues(_: TypedPipe[(a, b)], p.fn))
          case ((p: FlatMapped[a, b], bs), rec) =>
            mat.map(rec((p.input, bs | OnlyMapping)))(FlatMapped(_: TypedPipe[a], p.fn))
          case ((ForceToDisk(src@IterablePipe(_)), bs), rec) =>
            // no need to put a checkpoint here:
            rec((src, bs))
          case ((ForceToDisk(src@SourcePipe(_)), bs), rec) =>
            // no need to put a checkpoint here:
            rec((src, bs))
          case ((p: ForceToDisk[a], bs), rec) =>
            val newBs =
              if (isLogicalReduce(p.input)) OnlyMapping
              else Materialized
            val matP = rec((p.input, newBs))
            bs match {
              case Write =>
                // there is no need force to disk immediately before a write
                matP
              case _ => mat.materialize(matP)
            }
          case ((it@IterablePipe(_), _), _) =>
            mat.pure(it)
          case ((p: MapValues[a, b, c], bs), rec) =>
            mat.map(rec((p.input, bs | OnlyMapping)))(MapValues(_: TypedPipe[(a, b)], p.fn))
          case ((p: Mapped[a, b], bs), rec) =>
            mat.map(rec((p.input, bs | OnlyMapping)))(Mapped(_: TypedPipe[a], p.fn))
          case ((p: MergedTypedPipe[a], bs), rec) =>
            val mleft = rec((p.left, bs))
            val mright = rec((p.right, bs))
            val both = mat.zip(mleft, mright)
            mat.map(both) { case (l, r) => MergedTypedPipe(l, r) }
          case ((src@SourcePipe(_), _), _) =>
            mat.pure(src)
          case ((p: SumByLocalKeys[a, b], bs), rec) =>
            mat.map(rec((p.input, bs | OnlyMapping)))(SumByLocalKeys(_: TypedPipe[(a, b)], p.semigroup))
          case ((p: TrappedPipe[a], bs), rec) =>
            // TODO: it is a bit unclear if a trap is allowed on the back of a reduce?
            mat.map(rec((p.input, bs)))(TrappedPipe[a](_: TypedPipe[a], p.sink, p.conv))
          case ((p: WithDescriptionTypedPipe[a], bs), rec) =>
            mat.map(rec((p.input, bs)))(WithDescriptionTypedPipe(_: TypedPipe[a], p.descriptions))
          case ((p: WithOnComplete[a], bs), rec) =>
            mat.map(rec((p.input, bs)))(WithOnComplete(_: TypedPipe[a], p.fn))
          case ((EmptyTypedPipe, _), _) =>
            mat.pure(EmptyTypedPipe)
          case ((hg: HashCoGroup[a, b, c, d], bs), rec) =>
            val withBs = new FunctionK[TypedPipe, P] {
              def toFunction[A] = { tp => (tp, bs | OnlyMapping) }
            }
            // TODO: hashJoins may not be allowed in a reduce step in cascading,
            // not clear
            val recHG = FunctionK.andThen[TypedPipe, P, mat.TP](withBs, rec)
            handleHashCoGroup(hg, recHG)
          case ((CoGroupedPipe(cg), bs), rec) =>
            val withBs = new FunctionK[TypedPipe, P] {
              def toFunction[A] = { tp => (tp, bs | Materialized) }
            }
            // TODO: hashJoins may not be allowed in a reduce step in cascading,
            // not clear
            val recHG = FunctionK.andThen[TypedPipe, P, mat.TP](withBs, rec)
            val hcg = handleCoGrouped(cg, recHG)
            bs match {
              case BelowState.Materialized => mat.materialize(hcg)
              case _ => hcg
            }
          case ((ReduceStepPipe(rs), bs), rec) =>
            val withBs = new FunctionK[TypedPipe, P] {
              def toFunction[A] = { tp => (tp, bs | BelowState.Materialized) }
            }
            // TODO: hashJoins may not be allowed in a reduce step in cascading,
            // not clear
            val recHG = FunctionK.andThen[TypedPipe, P, mat.TP](withBs, rec)
            val hrs = handleReduceStep(rs, recHG)
            bs match {
              case BelowState.Materialized => mat.materialize(hrs)
              case _ => hrs
            }
        }
      })

    def write[A](p: PairK[Id, S, A]): (M[TypedPipe[A]], S[A]) = {
      val materialized: M[TypedPipe[A]] = fn((optDag.evaluate(p._1), BelowState.Write))
      (materialized, p._2)
    }

    writeIds.map(write(_))
  }
}
