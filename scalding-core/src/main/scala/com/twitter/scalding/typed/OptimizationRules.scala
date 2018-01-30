package com.twitter.scalding.typed

import com.stripe.dagon.{ FunctionK, Memoize, Rule, PartialRule, Dag, Literal }
import com.twitter.scalding.typed.functions.{ FlatMapping, FlatMappedFn, FilterKeysToFilter }
import com.twitter.scalding.typed.functions.ComposedFunctions.{ ComposedMapFn, ComposedFilterFn, ComposedOnComplete }

object OptimizationRules {
  type LiteralPipe[T] = Literal[TypedPipe, T]

  import Literal.{ Unary, Binary }
  import TypedPipe._

  /**
   * Since our TypedPipe is covariant, but the Literal is not
   * this is actually safe in this context, but not in general
   */
  def widen[T](l: LiteralPipe[_ <: T]): LiteralPipe[T] = {
    // to prove this is safe, see that if you have
    // LiteralPipe[_ <: T] we can call .evaluate to get
    // TypedPipe[_ <: T] which due to covariance is
    // TypedPipe[T], and then using toLiteral we can get
    // LiteralPipe[T]
    //
    // that would be wasteful to apply since the final
    // result is identity.
    l.asInstanceOf[LiteralPipe[T]]
  }

  /**
   * Convert a TypedPipe[T] to a Literal[TypedPipe, T] for
   * use with Dagon
   */
  def toLiteral: FunctionK[TypedPipe, LiteralPipe] =
    Memoize.functionK[TypedPipe, LiteralPipe](
      new Memoize.RecursiveK[TypedPipe, LiteralPipe] {

        def toFunction[A] = {
          case (cp: CounterPipe[a], f) =>
            Unary(f(cp.pipe), CounterPipe(_: TypedPipe[(a, Iterable[((String, String), Long)])]))
          case (c: CrossPipe[a, b], f) =>
            Binary(f(c.left), f(c.right), CrossPipe(_: TypedPipe[a], _: TypedPipe[b]))
          case (cv@CrossValue(_, _), f) =>
            def go[A, B](cv: CrossValue[A, B]): LiteralPipe[(A, B)] =
              cv match {
                case CrossValue(a, ComputedValue(v)) =>
                  Binary(f(a), f(v), { (a: TypedPipe[A], b: TypedPipe[B]) =>
                    CrossValue(a, ComputedValue(b))
                  })
                case CrossValue(a, v) =>
                  Unary(f(a), CrossValue(_: TypedPipe[A], v))
              }
            widen(go(cv))
          case (p: DebugPipe[a], f) =>
            Unary(f(p.input), DebugPipe(_: TypedPipe[a]))
          case (p: FilterKeys[a, b], f) =>
            widen(Unary(f(p.input), FilterKeys(_: TypedPipe[(a, b)], p.fn)))
          case (p: Filter[a], f) =>
            Unary(f(p.input), Filter(_: TypedPipe[a], p.fn))
          case (p: Fork[a], f) =>
            Unary(f(p.input), Fork(_: TypedPipe[a]))
          case (p: FlatMapValues[a, b, c], f) =>
            widen(Unary(f(p.input), FlatMapValues(_: TypedPipe[(a, b)], p.fn)))
          case (p: FlatMapped[a, b], f) =>
            Unary(f(p.input), FlatMapped(_: TypedPipe[a], p.fn))
          case (p: ForceToDisk[a], f) =>
            Unary(f(p.input), ForceToDisk(_: TypedPipe[a]))
          case (it@IterablePipe(_), _) =>
            Literal.Const(it)
          case (p: MapValues[a, b, c], f) =>
            widen(Unary(f(p.input), MapValues(_: TypedPipe[(a, b)], p.fn)))
          case (p: Mapped[a, b], f) =>
            Unary(f(p.input), Mapped(_: TypedPipe[a], p.fn))
          case (p: MergedTypedPipe[a], f) =>
            Binary(f(p.left), f(p.right), MergedTypedPipe(_: TypedPipe[a], _: TypedPipe[a]))
          case (src@SourcePipe(_), _) =>
            Literal.Const(src)
          case (p: SumByLocalKeys[a, b], f) =>
            widen(Unary(f(p.input), SumByLocalKeys(_: TypedPipe[(a, b)], p.semigroup)))
          case (p: TrappedPipe[a], f) =>
            Unary(f(p.input), TrappedPipe[a](_: TypedPipe[a], p.sink, p.conv))
          case (p: WithDescriptionTypedPipe[a], f) =>
            Unary(f(p.input), WithDescriptionTypedPipe(_: TypedPipe[a], p.description, p.deduplicate))
          case (p: WithOnComplete[a], f) =>
            Unary(f(p.input), WithOnComplete(_: TypedPipe[a], p.fn))
          case (EmptyTypedPipe, _) =>
            Literal.Const(EmptyTypedPipe)
          case (hg: HashCoGroup[a, b, c, d], f) =>
            widen(handleHashCoGroup(hg, f))
          case (CoGroupedPipe(cg), f) =>
            widen(handleCoGrouped(cg, f))
          case (ReduceStepPipe(rs), f) =>
            widen(handleReduceStep(rs, f))
        }
      })

    private def handleReduceStep[K, V1, V2](rs: ReduceStep[K, V1, V2], recurse: FunctionK[TypedPipe, LiteralPipe]): LiteralPipe[(K, V2)] =
      rs match {
        case step@IdentityReduce(_, _, _, _) =>
          Unary(widen[(K, V2)](recurse(step.mapped)), { (tp: TypedPipe[(K, V2)]) => ReduceStepPipe(step.copy(mapped = tp)) })
        case step@UnsortedIdentityReduce(_, _, _, _) =>
          Unary(widen[(K, V2)](recurse(step.mapped)), { (tp: TypedPipe[(K, V2)]) => ReduceStepPipe(step.copy(mapped = tp)) })
        case step@IdentityValueSortedReduce(_, _, _, _, _) =>
          def go[A, B](ivsr: IdentityValueSortedReduce[A, B]): LiteralPipe[(A, B)] =
            Unary(widen[(A, B)](recurse(ivsr.mapped)), { (tp: TypedPipe[(A, B)]) =>
              ReduceStepPipe[A, B, B](IdentityValueSortedReduce[A, B](
                ivsr.keyOrdering,
                tp,
                ivsr.valueSort,
                ivsr.reducers,
                ivsr.descriptions))
            })
          widen[(K, V2)](go(step))
        case step@ValueSortedReduce(_, _, _, _, _, _) =>
          def go[A, B, C](vsr: ValueSortedReduce[A, B, C]): LiteralPipe[(A, C)] =
            Unary(recurse(vsr.mapped), { (tp: TypedPipe[(A, B)]) =>
              ReduceStepPipe[A, B, C](ValueSortedReduce[A, B, C](
                vsr.keyOrdering,
                tp,
                vsr.valueSort,
                vsr.reduceFn,
                vsr.reducers,
                vsr.descriptions))
            })
          go(step)
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          def go[A, B, C](imr: IteratorMappedReduce[A, B, C]): LiteralPipe[(A, C)] =
            Unary(recurse(imr.mapped), { (tp: TypedPipe[(A, B)]) => ReduceStepPipe[A, B, C](imr.copy(mapped = tp)) })

          go(step)
      }

    private def handleCoGrouped[K, V](cg: CoGroupable[K, V], recurse: FunctionK[TypedPipe, LiteralPipe]): LiteralPipe[(K, V)] = {
      import CoGrouped._

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
          case kvPipe => IdentityReduce(cg.keyOrdering, kvPipe, None, Nil)
        }

      cg match {
        case p@Pair(_, _, _) =>
          def go[A, B, C](pair: Pair[K, A, B, C]): LiteralPipe[(K, C)] = {
            val llit = handleCoGrouped(pair.larger, recurse)
            val rlit = handleCoGrouped(pair.smaller, recurse)
            val fn = pair.fn
            Binary(llit, rlit, { (l: TypedPipe[(K, A)], r: TypedPipe[(K, B)]) =>
              Pair(pipeToCG(l), pipeToCG(r), fn)
            })
          }
          widen(go(p))
        case wr@WithReducers(_, _) =>
          def go[V1 <: V](wr: WithReducers[K, V1]): LiteralPipe[(K, V)] = {
            val reds = wr.reds
            Unary[TypedPipe, (K, V1), (K, V)](handleCoGrouped(wr.on, recurse), { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  withReducers(rs, reds)
                case CoGroupedPipe(cg) =>
                  CoGroupedPipe(WithReducers(cg, reds))
                case kvPipe =>
                  ReduceStepPipe(IdentityReduce(cg.keyOrdering, kvPipe, None, Nil)
                    .withReducers(reds))
              }
            })
          }
          go(wr)
        case wd@WithDescription(_, _) =>
          def go[V1 <: V](wd: WithDescription[K, V1]): LiteralPipe[(K, V)] = {
            val desc = wd.description
            Unary[TypedPipe, (K, V1), (K, V)](handleCoGrouped(wd.on, recurse), { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  withDescription(rs, desc)
                case CoGroupedPipe(cg) =>
                  CoGroupedPipe(WithDescription(cg, desc))
                case kvPipe =>
                  kvPipe.withDescription(desc)
              }
            })
          }
          go(wd)
        case fk@FilterKeys(_, _) =>
          def go[V1 <: V](fk: FilterKeys[K, V1]): LiteralPipe[(K, V)] = {
            val fn = fk.fn
            Unary[TypedPipe, (K, V1), (K, V)](handleCoGrouped(fk.on, recurse), { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  filterKeys(rs, fn)
                case CoGroupedPipe(cg) =>
                  CoGroupedPipe(FilterKeys(cg, fn))
                case kvPipe =>
                  kvPipe.filterKeys(fn)
              }
            })
          }
          go(fk)
        case mg@MapGroup(_, _) =>
          def go[V1, V2 <: V](mg: MapGroup[K, V1, V2]): LiteralPipe[(K, V)] = {
            val fn = mg.fn
            Unary[TypedPipe, (K, V1), (K, V)](handleCoGrouped(mg.on, recurse), { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  mapGroup(rs, fn)
                case CoGroupedPipe(cg) =>
                  CoGroupedPipe(MapGroup(cg, fn))
                case kvPipe =>
                  ReduceStepPipe(
                    IdentityReduce(cg.keyOrdering, kvPipe, None, Nil)
                      .mapGroup(fn))
              }
            })
          }
          go(mg)
        case step@IdentityReduce(_, _, _, _) =>
          widen(handleReduceStep(step, recurse))
        case step@UnsortedIdentityReduce(_, _, _, _) =>
          widen(handleReduceStep(step, recurse))
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          widen(handleReduceStep(step, recurse))
      }
    }

    /**
     * This can't really usefully be on ReduceStep since users never want to use it
     * as an ADT, as the planner does.
     */
    private def withReducers[K, V1, V2](rs: ReduceStep[K, V1, V2], reds: Int): TypedPipe[(K, V2)] =
      rs match {
        case step@IdentityReduce(_, _, _, _) =>
          ReduceStepPipe(step.withReducers(reds))
        case step@UnsortedIdentityReduce(_, _, _, _) =>
          ReduceStepPipe(step.withReducers(reds))
        case step@IdentityValueSortedReduce(_, _, _, _, _) =>
          ReduceStepPipe(step.withReducers(reds))
        case step@ValueSortedReduce(_, _, _, _, _, _) =>
          ReduceStepPipe(step.withReducers(reds))
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          ReduceStepPipe(step.withReducers(reds))
      }

    private def withDescription[K, V1, V2](rs: ReduceStep[K, V1, V2], descr: String): TypedPipe[(K, V2)] =
      rs match {
        case step@IdentityReduce(_, _, _, _) =>
          ReduceStepPipe(step.withDescription(descr))
        case step@UnsortedIdentityReduce(_, _, _, _) =>
          ReduceStepPipe(step.withDescription(descr))
        case step@IdentityValueSortedReduce(_, _, _, _, _) =>
          ReduceStepPipe(step.withDescription(descr))
        case step@ValueSortedReduce(_, _, _, _, _, _) =>
          ReduceStepPipe(step.withDescription(descr))
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          ReduceStepPipe(step.withDescription(descr))
      }

    private def filterKeys[K, V1, V2](rs: ReduceStep[K, V1, V2], fn: K => Boolean): TypedPipe[(K, V2)] =
      rs match {
        case IdentityReduce(ord, p, r, d) =>
          ReduceStepPipe(IdentityReduce(ord, FilterKeys(p, fn), r, d))
        case UnsortedIdentityReduce(ord, p, r, d) =>
          ReduceStepPipe(UnsortedIdentityReduce(ord, FilterKeys(p, fn), r, d))
        case ivsr@IdentityValueSortedReduce(_, _, _, _, _) =>
          def go[V](ivsr: IdentityValueSortedReduce[K, V]): TypedPipe[(K, V)] = {
            val IdentityValueSortedReduce(ord, p, v, r, d) = ivsr
            ReduceStepPipe(IdentityValueSortedReduce[K, V](ord, FilterKeys(p, fn), v, r, d))
          }
          go(ivsr)
        case vsr@ValueSortedReduce(_, _, _, _, _, _) =>
          def go(vsr: ValueSortedReduce[K, V1, V2]): TypedPipe[(K, V2)] = {
            val ValueSortedReduce(ord, p, v, redfn, r, d) = vsr
            ReduceStepPipe(ValueSortedReduce[K, V1, V2](ord, FilterKeys(p, fn), v, redfn, r, d))
          }
          go(vsr)
        case imr@IteratorMappedReduce(_, _, _, _, _) =>
          def go(imr: IteratorMappedReduce[K, V1, V2]): TypedPipe[(K, V2)] = {
            val IteratorMappedReduce(ord, p, redfn, r, d) = imr
            ReduceStepPipe(IteratorMappedReduce[K, V1, V2](ord, FilterKeys(p, fn), redfn, r, d))
          }
          go(imr)
      }

    private def mapGroup[K, V1, V2, V3](rs: ReduceStep[K, V1, V2], fn: (K, Iterator[V2]) => Iterator[V3]): TypedPipe[(K, V3)] =
      rs match {
        case step@IdentityReduce(_, _, _, _) =>
          ReduceStepPipe(step.mapGroup(fn))
        case step@UnsortedIdentityReduce(_, _, _, _) =>
          ReduceStepPipe(step.mapGroup(fn))
        case step@IdentityValueSortedReduce(_, _, _, _, _) =>
          ReduceStepPipe(step.mapGroup(fn))
        case step@ValueSortedReduce(_, _, _, _, _, _) =>
          ReduceStepPipe(step.mapGroup(fn))
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          ReduceStepPipe(step.mapGroup(fn))
      }

    private def handleHashCoGroup[K, V, V2, R](hj: HashCoGroup[K, V, V2, R], recurse: FunctionK[TypedPipe, LiteralPipe]): LiteralPipe[(K, R)] = {
      val rightLit: LiteralPipe[(K, V2)] = hj.right match {
        case step@IdentityReduce(_, _, _, _) =>
          Unary(widen[(K, V2)](recurse(step.mapped)), { (tp: TypedPipe[(K, V2)]) => ReduceStepPipe(step.copy(mapped = tp)) })
        case step@UnsortedIdentityReduce(_, _, _, _) =>
          Unary(widen[(K, V2)](recurse(step.mapped)), { (tp: TypedPipe[(K, V2)]) => ReduceStepPipe(step.copy(mapped = tp)) })
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          def go[A, B, C](imr: IteratorMappedReduce[A, B, C]): LiteralPipe[(A, C)] =
            Unary(recurse(imr.mapped), { (tp: TypedPipe[(A, B)]) => ReduceStepPipe[A, B, C](imr.copy(mapped = tp)) })

          widen(go(step))
      }

      val ordK: Ordering[K] = hj.right match {
        case step@IdentityReduce(_, _, _, _) => step.keyOrdering
        case step@UnsortedIdentityReduce(_, _, _, _) => step.keyOrdering
        case step@IteratorMappedReduce(_, _, _, _, _) => step.keyOrdering
      }

      val joiner = hj.joiner

      Binary(recurse(hj.left), rightLit,
        { (ltp: TypedPipe[(K, V)], rtp: TypedPipe[(K, V2)]) =>
          rtp match {
            case ReduceStepPipe(hg: HashJoinable[K @unchecked, V2 @unchecked]) =>
              HashCoGroup(ltp, hg, joiner)
            case otherwise =>
              HashCoGroup(ltp, IdentityReduce(ordK, otherwise, None, Nil), joiner)
          }
        })
    }

  /**
   * Unroll a set of merges up to the first non-merge node, dropping
   * an EmptyTypedPipe from the list
   */
  def unrollMerge[A](t: TypedPipe[A]): List[TypedPipe[A]] = {
    @annotation.tailrec
    def loop(first: TypedPipe[A], todo: List[TypedPipe[A]], acc: List[TypedPipe[A]]): List[TypedPipe[A]] =
      first match {
        case MergedTypedPipe(l, r) => loop(l, r :: todo, acc)
        case EmptyTypedPipe =>
          todo match {
            case Nil => acc.reverse
            case h :: tail => loop(h, tail, acc)
          }
        case notMerge =>
          val acc1 = notMerge :: acc
          todo match {
            case Nil => acc1.reverse
            case h :: tail => loop(h, tail, acc1)
          }
      }

    loop(t, Nil, Nil)
  }

  /////////////////////////////
  //
  // Here are some actual rules for simplifying TypedPipes
  //
  /////////////////////////////

  /**
   * It is easier for planning if all fanouts are made explicit.
   * This rule adds a Fork node every time there is a fanout
   *
   * This rule applied first makes it easier to match in subsequent
   * rules without constantly checking for fanout nodes.
   *
   * This can increase the number of map-reduce steps compared
   * to simply recomputing on both sides of a fork
   */
  object AddExplicitForks extends Rule[TypedPipe] {

    def maybeFork[A](on: Dag[TypedPipe], t: TypedPipe[A]): Option[TypedPipe[A]] =
      t match {
        case ForceToDisk(_) => None
        case Fork(t) if on.contains(ForceToDisk(t)) => Some(ForceToDisk(t))
        case Fork(_) => None
        case EmptyTypedPipe | IterablePipe(_) | SourcePipe(_) => None
        case other if !on.hasSingleDependent(other) =>
          Some {
            // if we are already forcing this, use it
            if (on.contains(ForceToDisk(other))) ForceToDisk(other)
            else Fork(other)
          }
        case _ => None
      }

    def needsFork[A](on: Dag[TypedPipe], t: TypedPipe[A]): Boolean =
      maybeFork(on, t).isDefined

    private def forkCoGroup[K, V](on: Dag[TypedPipe], cg: CoGrouped[K, V]): Option[CoGrouped[K, V]] = {
      import CoGrouped._

      cg match {
        case Pair(left: HashJoinable[K, v], right, jf) if forkHashJoinable(on, left).isDefined =>
          forkHashJoinable(on, left).map {
            Pair(_, right, jf)
          }
        case Pair(left: CoGrouped[K, v], right, jf) if forkCoGroup(on, left).isDefined =>
          forkCoGroup(on, left).map {
            Pair(_, right, jf)
          }
        case Pair(left, right: HashJoinable[K, v], jf) if forkHashJoinable(on, right).isDefined =>
          forkHashJoinable(on, right).map {
            Pair(left, _, jf)
          }
        case Pair(left, right: CoGrouped[K, v], jf) if forkCoGroup(on, right).isDefined =>
          forkCoGroup(on, right).map {
            Pair(left, _, jf)
          }
        case Pair(_, _, _) => None // neither side needs a fork
        case WithDescription(cg, d) => forkCoGroup(on, cg).map(WithDescription(_, d))
        case WithReducers(cg, r) => forkCoGroup(on, cg).map(WithReducers(_, r))
        case MapGroup(cg, fn) => forkCoGroup(on, cg).map(MapGroup(_, fn))
        case FilterKeys(cg, fn) => forkCoGroup(on, cg).map(FilterKeys(_, fn))
      }
    }

    /**
     * The casts in here are safe, but scala loses track of the types in these kinds of
     * pattern matches.
     * We can fix it by changing the types on the identity reduces to use EqTypes[V1, V2]
     * in case class and leaving the V2 parameter.
     */
    private def forkReduceStep[A, B, C](on: Dag[TypedPipe], rs: ReduceStep[A, B, C]): Option[ReduceStep[A, B, C]] = rs match {
      case step@IdentityReduce(_, _, _, _) =>
        maybeFork(on, step.mapped).map { p => step.copy(mapped = p) }.asInstanceOf[Option[ReduceStep[A, B, C]]]
      case step@UnsortedIdentityReduce(_, _, _, _) =>
        maybeFork(on, step.mapped).map { p => step.copy(mapped = p) }.asInstanceOf[Option[ReduceStep[A, B, C]]]
      case step@IdentityValueSortedReduce(_, _, _, _, _) =>
        maybeFork(on, step.mapped).map { p => step.copy(mapped = p) }.asInstanceOf[Option[ReduceStep[A, B, C]]]
      case step@ValueSortedReduce(_, _, _, _, _, _) =>
        def go(vsr: ValueSortedReduce[A, B, C]): Option[ValueSortedReduce[A, B, C]] =
          maybeFork(on, step.mapped).map { p =>
            ValueSortedReduce[A, B, C](vsr.keyOrdering,
              p, vsr.valueSort, vsr.reduceFn, vsr.reducers, vsr.descriptions)
          }
        go(step)
      case step@IteratorMappedReduce(_, _, _, _, _) =>
        def go(imr: IteratorMappedReduce[A, B, C]): Option[IteratorMappedReduce[A, B, C]] =
          maybeFork(on, step.mapped).map { p => imr.copy(mapped = p) }
        go(step)
    }

    private def forkHashJoinable[K, V](on: Dag[TypedPipe], hj: HashJoinable[K, V]): Option[HashJoinable[K, V]] =
      hj match {
        case step@IdentityReduce(_, _, _, _) =>
          maybeFork(on, step.mapped).map { p => step.copy(mapped = p) }
        case step@UnsortedIdentityReduce(_, _, _, _) =>
          maybeFork(on, step.mapped).map { p => step.copy(mapped = p) }
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          maybeFork(on, step.mapped).map { p => step.copy(mapped = p) }
      }

    def apply[T](on: Dag[TypedPipe]) = {
      case CounterPipe(a) if needsFork(on, a) => maybeFork(on, a).map(CounterPipe(_))
      case CrossPipe(a, b) if needsFork(on, a) => maybeFork(on, a).map(CrossPipe(_, b))
      case CrossPipe(a, b) if needsFork(on, b) => maybeFork(on, b).map(CrossPipe(a, _))
      case CrossValue(a, b) if needsFork(on, a) => maybeFork(on, a).map(CrossValue(_, b))
      case CrossValue(a, ComputedValue(b)) if needsFork(on, b) => maybeFork(on, b).map { fb => CrossValue(a, ComputedValue(fb)) }
      case DebugPipe(p) => maybeFork(on, p).map(DebugPipe(_))
      case FilterKeys(p, fn) => maybeFork(on, p).map(FilterKeys(_, fn))
      case f@Filter(_, _) =>
        def go[A](f: Filter[A]): Option[TypedPipe[A]] = {
          val Filter(p, fn) = f
          maybeFork(on, p).map(Filter(_, fn))
        }
        go(f)
      case FlatMapValues(p, fn) => maybeFork(on, p).map(FlatMapValues(_, fn))
      case FlatMapped(p, fn) => maybeFork(on, p).map(FlatMapped(_, fn))
      case ForceToDisk(_) | Fork(_) => None // already has a barrier
      case HashCoGroup(left, right, jf) if needsFork(on, left) => maybeFork(on, left).map(HashCoGroup(_, right, jf))
      case HashCoGroup(left, right, jf) => forkHashJoinable(on, right).map(HashCoGroup(left, _, jf))
      case MapValues(p, fn) => maybeFork(on, p).map(MapValues(_, fn))
      case Mapped(p, fn) => maybeFork(on, p).map(Mapped(_, fn))
      case MergedTypedPipe(a, b) if needsFork(on, a) => maybeFork(on, a).map(MergedTypedPipe(_, b))
      case MergedTypedPipe(a, b) if needsFork(on, b) => maybeFork(on, b).map(MergedTypedPipe(a, _))
      case ReduceStepPipe(rs) => forkReduceStep(on, rs).map(ReduceStepPipe(_))
      case SumByLocalKeys(p, sg) => maybeFork(on, p).map(SumByLocalKeys(_, sg))
      case t@TrappedPipe(_, _, _) =>
        def go[A](t: TrappedPipe[A]): Option[TypedPipe[A]] = {
          val TrappedPipe(p, sink, conv) = t
          maybeFork(on, p).map(TrappedPipe(_, sink, conv))
        }
        go(t)
      case CoGroupedPipe(cgp) => forkCoGroup(on, cgp).map(CoGroupedPipe(_))
      case WithOnComplete(p, fn) => maybeFork(on, p).map(WithOnComplete(_, fn))
      case WithDescriptionTypedPipe(p, d1, d2) => maybeFork(on, p).map(WithDescriptionTypedPipe(_, d1, d2))
      case _ => None
    }
  }

  /**
   * a.flatMap(f).flatMap(g) == a.flatMap { x => f(x).flatMap(g) }
   */
  object ComposeFlatMap extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case FlatMapped(FlatMapped(in, fn0), fn1) =>
        FlatMapped(in, FlatMappedFn(fn1).runAfter(FlatMapping.FlatM(fn0)))
      case FlatMapValues(FlatMapValues(in, fn0), fn1) =>
        FlatMapValues(in, FlatMappedFn(fn1).runAfter(FlatMapping.FlatM(fn0)))
    }
  }

  /**
   * a.map(f).map(g) == a.map { x => f(x).map(g) }
   */
  object ComposeMap extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case Mapped(Mapped(in, fn0), fn1) =>
        Mapped(in, ComposedMapFn(fn0, fn1))
      case MapValues(MapValues(in, fn0), fn1) =>
        MapValues(in, ComposedMapFn(fn0, fn1))
    }
  }

  /**
   * a.filter(f).filter(g) == a.filter { x => f(x) && g(x) }
   *
   * also if a filterKeys follows a filter, we might as well
   * compose because we can't push the filterKeys up higher
   */
  object ComposeFilter extends Rule[TypedPipe] {
    def apply[T](on: Dag[TypedPipe]) = {
      // scala can't type check this, so we hold its hand:
      // case Filter(Filter(in, fn0), fn1) =>
      //   Some(Filter(in, ComposedFilterFn(fn0, fn1)))
      case f@Filter(_, _) =>
        def go[A](f: Filter[A]): Option[TypedPipe[A]] =
          f.input match {
            case f1: Filter[a] =>
              // We have to be really careful here because f.fn and f1.fn
              // have the same type. Type checking won't save you here
              // we do have a test that exercises this, however
              Some(Filter[a](f1.input, ComposedFilterFn(f1.fn, f.fn)))
            case _ => None
          }
        go(f)
      case FilterKeys(FilterKeys(in, fn0), fn1) =>
        Some(FilterKeys(in, ComposedFilterFn(fn0, fn1)))
      case FilterKeys(Filter(in, fn0), fn1) =>
        Some(Filter(in, ComposedFilterFn(fn0, FilterKeysToFilter(fn1))))
      case _ => None
    }
  }

  /**
   * a.onComplete(f).onComplete(g) == a.onComplete { () => f(); g() }
   */
  object ComposeWithOnComplete extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case WithOnComplete(WithOnComplete(pipe, fn0), fn1) =>
        WithOnComplete(pipe, ComposedOnComplete(fn0, fn1))
    }
  }
  /**
   * a.map(f).flatMap(g) == a.flatMap { x => g(f(x)) }
   * a.flatMap(f).map(g) == a.flatMap { x => f(x).map(g) }
   *
   * This is a rule you may want to apply after having
   * composed all the maps first
   */
  object ComposeMapFlatMap extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case FlatMapped(Mapped(in, f), g) =>
        FlatMapped(in, FlatMappedFn(g).runAfter(FlatMapping.Map(f)))
      case FlatMapValues(MapValues(in, f), g) =>
        FlatMapValues(in, FlatMappedFn(g).runAfter(FlatMapping.Map(f)))
      case Mapped(FlatMapped(in, f), g) =>
        FlatMapped(in, FlatMappedFn(f).combine(FlatMappedFn.fromMap(g)))
      case MapValues(FlatMapValues(in, f), g) =>
        FlatMapValues(in, FlatMappedFn(f).combine(FlatMappedFn.fromMap(g)))
    }
  }


  /**
   * a.filter(f).flatMap(g) == a.flatMap { x => if (f(x)) g(x) else Iterator.empty }
   * a.flatMap(f).filter(g) == a.flatMap { x => f(x).filter(g) }
   *
   * This is a rule you may want to apply after having
   * composed all the filters first
   */
  object ComposeFilterFlatMap extends Rule[TypedPipe] {
    def apply[T](on: Dag[TypedPipe]) = {
      case FlatMapped(Filter(in, f), g) =>
        Some(FlatMapped(in, FlatMappedFn(g).runAfter(FlatMapping.filter(f))))
      case filter: Filter[b] =>
        filter.input match {
          case fm: FlatMapped[a, b] =>
            Some(FlatMapped[a, b](fm.input, FlatMappedFn(fm.fn).combine(FlatMappedFn.fromFilter(filter.fn))))
          case _ => None
        }
      case _ =>
        None
    }
  }
  /**
   * a.filter(f).map(g) == a.flatMap { x => if (f(x)) Iterator.single(g(x)) else Iterator.empty }
   * a.map(f).filter(g) == a.flatMap { x => val y = f(x); if (g(y)) Iterator.single(y) else Iterator.empty }
   *
   * This is a rule you may want to apply after having
   * composed all the filters first
   *
   * This may be a deoptimization on some platforms that have native filters since
   * you could avoid the Iterator boxing in that case.
   */
  object ComposeFilterMap extends Rule[TypedPipe] {
    def apply[T](on: Dag[TypedPipe]) = {
      case Mapped(Filter(in, f), g) =>
        Some(FlatMapped(in, FlatMappedFn.fromFilter(f).combine(FlatMappedFn.fromMap(g))))
      case filter: Filter[b] =>
        filter.input match {
          case fm: Mapped[a, b] =>
            Some(FlatMapped[a, b](fm.input, FlatMappedFn.fromMap(fm.fn).combine(FlatMappedFn.fromFilter(filter.fn))))
          case _ => None
        }
      case _ =>
        None
    }
  }

  /**
   * In scalding 0.17 and earlier, descriptions were automatically pushdown below
   * merges and flatMaps/map/etc..
   */
  object DescribeLater extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case Mapped(WithDescriptionTypedPipe(in, desc, dedup), fn) =>
        WithDescriptionTypedPipe(Mapped(in, fn), desc, dedup)
      case MapValues(WithDescriptionTypedPipe(in, desc, dedup), fn) =>
        WithDescriptionTypedPipe(MapValues(in, fn), desc, dedup)
      case FlatMapped(WithDescriptionTypedPipe(in, desc, dedup), fn) =>
        WithDescriptionTypedPipe(FlatMapped(in, fn), desc, dedup)
      case FlatMapValues(WithDescriptionTypedPipe(in, desc, dedup), fn) =>
        WithDescriptionTypedPipe(FlatMapValues(in, fn), desc, dedup)
      case f@Filter(WithDescriptionTypedPipe(_, _, _), _) =>
        def go[A](f: Filter[A]): TypedPipe[A] =
          f match {
            case Filter(WithDescriptionTypedPipe(in, desc, dedup), fn) =>
              WithDescriptionTypedPipe(Filter(in, fn), desc, dedup)
            case unreachable => unreachable
          }
        go(f)
      case FilterKeys(WithDescriptionTypedPipe(in, desc, dedup), fn) =>
        WithDescriptionTypedPipe(FilterKeys(in, fn), desc, dedup)
    }
  }

  /**
   * (a ++ a) == a.flatMap { t => List(t, t) }
   */
  object DiamondToFlatMap extends Rule[TypedPipe] {
    def apply[T](on: Dag[TypedPipe]) = {
      case m@MergedTypedPipe(_, _) =>
        val pipes = unrollMerge(m)
        val flatMapped =
          pipes.groupBy { tp => tp: TypedPipe[T] }
            .iterator
            .map {
              case (p, Nil) => sys.error(s"unreachable: $p has no values")
              case (p, _ :: Nil) => p // just once
              case (p, repeated) =>
                val rsize = repeated.size
                p.flatMap(Iterator.fill(rsize)(_))
            }
            .toVector

        if (pipes.size == flatMapped.size) None // we didn't reduce the number of merges
        else Some(TypedPipe.typedPipeMonoid.sum(flatMapped))
      case _ => None
    }
  }

  /**
   * After a forceToDisk there is no need to immediately fork.
   * Calling forceToDisk twice in a row is the same as once.
   * Calling fork twice in a row is the same as once.
   */
  object RemoveDuplicateForceFork extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case ForceToDisk(ForceToDisk(t)) => ForceToDisk(t)
      case ForceToDisk(Fork(t)) => ForceToDisk(t)
      case Fork(Fork(t)) => Fork(t)
      case Fork(ForceToDisk(t)) => ForceToDisk(t)
      case Fork(t) if on.contains(ForceToDisk(t)) => ForceToDisk(t)
    }
  }

  /**
   * We ignore .group if there are is no setting of reducers
   *
   * This is arguably not a great idea, but scalding has always
   * done it to minimize accidental map-reduce steps
   */
  object IgnoreNoOpGroup extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case ReduceStepPipe(IdentityReduce(_, input, None, _)) =>
        input
    }
  }

  /**
   * In map-reduce settings, Merge is almost free in two contexts:
   * 1. the final write
   * 2. at the point we are doing a shuffle anyway.
   *
   * By defering merge as long as possible, we hope to find more such
   * cases
   */
  object DeferMerge extends PartialRule[TypedPipe] {
    private def handleFilter[A]: PartialFunction[Filter[A], TypedPipe[A]] = {
      case Filter(MergedTypedPipe(a, b), fn) => MergedTypedPipe(Filter(a, fn), Filter(b, fn))
    }

    def applyWhere[T](on: Dag[TypedPipe]) = {
      case Mapped(MergedTypedPipe(a, b), fn) =>
        MergedTypedPipe(Mapped(a, fn), Mapped(b, fn))
      case FlatMapped(MergedTypedPipe(a, b), fn) =>
        MergedTypedPipe(FlatMapped(a, fn), FlatMapped(b, fn))
      case MapValues(MergedTypedPipe(a, b), fn) =>
        MergedTypedPipe(MapValues(a, fn), MapValues(b, fn))
      case FlatMapValues(MergedTypedPipe(a, b), fn) =>
        MergedTypedPipe(FlatMapValues(a, fn), FlatMapValues(b, fn))
      case f@Filter(_, _) if handleFilter.isDefinedAt(f) => handleFilter(f)
      case FilterKeys(MergedTypedPipe(a, b), fn) =>
        MergedTypedPipe(FilterKeys(a, fn), FilterKeys(b, fn))
    }
  }

  /**
   * Push filterKeys up as early as possible. This can happen before
   * a shuffle, which can be a major win. This allows you to write
   * generic methods that return all the data, but if downstream someone
   * only wants certain keys they don't pay to compute everything.
   *
   * This is an optimization we didn't do in scalding 0.17 and earlier
   * because .toTypedPipe on the group totally hid the structure from
   * us
   */
  object FilterKeysEarly extends Rule[TypedPipe] {
    private def filterReduceStep[K, V1, V2](rs: ReduceStep[K, V1, V2], fn: K => Boolean): ReduceStep[K, _, _ <: V2] =
      rs match {
        case step@IdentityReduce(_, _, _, _) => step.filterKeys(fn)
        case step@UnsortedIdentityReduce(_, _, _, _) => step.filterKeys(fn)
        case step@IdentityValueSortedReduce(_, _, _, _, _) => step.filterKeys(fn)
        case step@ValueSortedReduce(_, _, _, _, _, _) => step.filterKeys(fn)
        case step@IteratorMappedReduce(_, _, _, _, _) => step.filterKeys(fn)
      }

    private def filterCoGroupable[K, V](rs: CoGroupable[K, V], fn: K => Boolean): CoGroupable[K, V] =
      rs match {
        case step@IdentityReduce(_, _, _, _) => step.filterKeys(fn)
        case step@UnsortedIdentityReduce(_, _, _, _) => step.filterKeys(fn)
        case step@IteratorMappedReduce(_, _, _, _, _) => step.filterKeys(fn)
        case cg: CoGrouped[K, V] => filterCoGroup(cg, fn)
      }

    private def filterCoGroup[K, V](cg: CoGrouped[K, V], fn: K => Boolean): CoGrouped[K, V] =
      cg match {
        case CoGrouped.Pair(a, b, jf) =>
          CoGrouped.Pair(filterCoGroupable(a, fn), filterCoGroupable(b, fn), jf)
        case CoGrouped.FilterKeys(cg, g) =>
          filterCoGroup(cg, ComposedFilterFn(g, fn))
        case CoGrouped.MapGroup(cg, g) =>
          CoGrouped.MapGroup(filterCoGroup(cg, fn), g)
        case CoGrouped.WithDescription(cg, d) =>
          CoGrouped.WithDescription(filterCoGroup(cg, fn), d)
        case CoGrouped.WithReducers(cg, r) =>
          CoGrouped.WithReducers(filterCoGroup(cg, fn), r)
      }

    def apply[T](on: Dag[TypedPipe]) = {
      case FilterKeys(ReduceStepPipe(rsp), fn) =>
        Some(ReduceStepPipe(filterReduceStep(rsp, fn)))
      case FilterKeys(CoGroupedPipe(cg), fn) =>
        Some(CoGroupedPipe(filterCoGroup(cg, fn)))
      case FilterKeys(HashCoGroup(left, right, joiner), fn) =>
        val newRight = right match {
          case step@IdentityReduce(_, _, _, _) => step.filterKeys(fn)
          case step@UnsortedIdentityReduce(_, _, _, _) => step.filterKeys(fn)
          case step@IteratorMappedReduce(_, _, _, _, _) => step.filterKeys(fn)
        }
        Some(HashCoGroup(FilterKeys(left, fn), newRight, joiner))
      case FilterKeys(MapValues(pipe, mapFn), filterFn) =>
        Some(MapValues(FilterKeys(pipe, filterFn), mapFn))
      case FilterKeys(FlatMapValues(pipe, fmFn), filterFn) =>
        Some(FlatMapValues(FilterKeys(pipe, filterFn), fmFn))
      case _ => None
    }
  }

  /**
   * EmptyTypedPipe is kind of zero of most of these operations
   * We go ahead and simplify as much as possible if we see
   * an EmptyTypedPipe
   */
  object EmptyIsOftenNoOp extends PartialRule[TypedPipe] {

    private def emptyCogroup[K, V](cg: CoGrouped[K, V]): Boolean = {
      import CoGrouped._

      def empty(t: TypedPipe[Any]): Boolean = t match {
        case EmptyTypedPipe => true
        case _ => false
      }
      cg match {
        case Pair(left, _, jf) if left.inputs.forall(empty) && (Joiner.isLeftJoinLike(jf) == Some(true)) => true
        case Pair(_, right, jf) if right.inputs.forall(empty) && (Joiner.isRightJoinLike(jf) == Some(true)) => true
        case Pair(left, right, _) if left.inputs.forall(empty) && right.inputs.forall(empty) => true
        case Pair(_, _, _) => false
        case WithDescription(cg, _) => emptyCogroup(cg)
        case WithReducers(cg, _) => emptyCogroup(cg)
        case MapGroup(cg, _) => emptyCogroup(cg)
        case FilterKeys(cg, _) => emptyCogroup(cg)
      }
    }

    private def emptyHashJoinable[K, V](hj: HashJoinable[K, V]): Boolean =
      hj match {
        case step@IdentityReduce(_, _, _, _) => step.mapped == EmptyTypedPipe
        case step@UnsortedIdentityReduce(_, _, _, _) => step.mapped == EmptyTypedPipe
        case step@IteratorMappedReduce(_, _, _, _, _) => step.mapped == EmptyTypedPipe
      }

    def applyWhere[T](on: Dag[TypedPipe]) = {
      case CrossPipe(EmptyTypedPipe, _) => EmptyTypedPipe
      case CrossPipe(_, EmptyTypedPipe) => EmptyTypedPipe
      case CrossValue(EmptyTypedPipe, _) => EmptyTypedPipe
      case CrossValue(_, ComputedValue(EmptyTypedPipe)) => EmptyTypedPipe
      case CrossValue(_, EmptyValue) => EmptyTypedPipe
      case DebugPipe(EmptyTypedPipe) => EmptyTypedPipe
      case FilterKeys(EmptyTypedPipe, _) => EmptyTypedPipe
      case Filter(EmptyTypedPipe, _) => EmptyTypedPipe
      case FlatMapValues(EmptyTypedPipe, _) => EmptyTypedPipe
      case FlatMapped(EmptyTypedPipe, _) => EmptyTypedPipe
      case ForceToDisk(EmptyTypedPipe) => EmptyTypedPipe
      case Fork(EmptyTypedPipe) => EmptyTypedPipe
      case HashCoGroup(EmptyTypedPipe, _, _) => EmptyTypedPipe
      case HashCoGroup(_, right, hjf) if emptyHashJoinable(right) && Joiner.isInnerHashJoinLike(hjf) == Some(true) => EmptyTypedPipe
      case MapValues(EmptyTypedPipe, _) => EmptyTypedPipe
      case Mapped(EmptyTypedPipe, _) => EmptyTypedPipe
      case MergedTypedPipe(EmptyTypedPipe, a) => a
      case MergedTypedPipe(a, EmptyTypedPipe) => a
      case ReduceStepPipe(rs: ReduceStep[_, _, _]) if rs.mapped == EmptyTypedPipe => EmptyTypedPipe
      case SumByLocalKeys(EmptyTypedPipe, _) => EmptyTypedPipe
      case TrappedPipe(EmptyTypedPipe, _, _) => EmptyTypedPipe
      case CoGroupedPipe(cgp) if emptyCogroup(cgp) => EmptyTypedPipe
      case WithOnComplete(EmptyTypedPipe, _) => EmptyTypedPipe // there is nothing to do, so we never have workers complete
      case WithDescriptionTypedPipe(EmptyTypedPipe, _, _) => EmptyTypedPipe // descriptions apply to tasks, but empty has no tasks
    }
  }

  /**
   * If an Iterable is empty, it is the same as EmptyTypedPipe
   */
  object EmptyIterableIsEmpty extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case IterablePipe(it) if it.isEmpty => EmptyTypedPipe
    }
  }

  /**
   * This is useful on map-reduce like systems to avoid
   * serializing data into the system that you are going
   * to then filter
   */
  object FilterLocally extends Rule[TypedPipe] {
    def apply[T](on: Dag[TypedPipe]) = {
      case f@Filter(_, _) =>
        def go[T1 <: T](f: Filter[T1]): Option[TypedPipe[T]] =
          f match {
            case Filter(IterablePipe(iter), fn) =>
              Some(IterablePipe(iter.filter(fn)))
            case _ => None
          }
        go(f)
      case f@FilterKeys(_, _) =>
        def go[K, V, T >: (K, V)](f: FilterKeys[K, V]): Option[TypedPipe[T]] =
          f match {
            case FilterKeys(IterablePipe(iter), fn) =>
              Some(IterablePipe(iter.filter { case (k, _) => fn(k) }))
            case _ => None
          }
        go(f)
      case _ => None
    }
  }
  /**
   * ForceToDisk before hashJoin, this makes sure any filters
   * have been applied
   */
  object ForceToDiskBeforeHashJoin extends Rule[TypedPipe] {
    // A set of operations naturally have barriers after them,
    // there is no need to add an explicit force after a reduce
    // step or after a source, since both will already have been
    // checkpointed
    final def maybeForce[T](t: TypedPipe[T]): TypedPipe[T] =
      t match {
        case SourcePipe(_) | IterablePipe(_) | CoGroupedPipe(_) | ReduceStepPipe(_) | ForceToDisk(_) => t
        case WithOnComplete(pipe, fn) =>
          WithOnComplete(maybeForce(pipe), fn)
        case WithDescriptionTypedPipe(pipe, desc, dedup) =>
          WithDescriptionTypedPipe(maybeForce(pipe), desc, dedup)
        case pipe => ForceToDisk(pipe)
      }

    def apply[T](on: Dag[TypedPipe]) = {
      case HashCoGroup(left, right: HashJoinable[a, b], joiner) =>
        val newRight: HashJoinable[a, b] = right match {
          case step@IdentityReduce(_, _, _, _) =>
            step.copy(mapped = maybeForce(step.mapped))
          case step@UnsortedIdentityReduce(_, _, _, _) =>
            step.copy(mapped = maybeForce(step.mapped))
          case step@IteratorMappedReduce(_, _, _, _, _) =>
            step.copy(mapped = maybeForce(step.mapped))
        }
        if (newRight != right) Some(HashCoGroup(left, newRight, joiner))
        else None
      case _ => None
    }
  }

  ///////
  // These are composed rules that are related
  //////

  /**
   * Like kinds can be composed .map(f).map(g),
   * filter(f).filter(g) etc...
   */
  val composeSame: Rule[TypedPipe] =
    Rule.orElse(
      List(
        ComposeMap,
        ComposeFilter,
        ComposeFlatMap,
        ComposeWithOnComplete))
  /**
   * If you are going to do a flatMap, following it or preceding it with map/filter
   * you might as well compose into the flatMap
   */
  val composeIntoFlatMap: Rule[TypedPipe] =
    Rule.orElse(
      List(
        ComposeMapFlatMap,
        ComposeFilterFlatMap,
        ComposeFlatMap))

  val simplifyEmpty: Rule[TypedPipe] =
    EmptyIsOftenNoOp.orElse(
      EmptyIterableIsEmpty)

  /**
   * These are a list of rules to be applied in order (Dag.applySeq)
   * that should generally always improve things on Map/Reduce-like
   * platforms.
   *
   * These are rules we should apply to any TypedPipe before handing
   * to cascading. These should be a bit conservative in that they
   * should be highly likely to improve the graph.
   */
  val standardMapReduceRules: List[Rule[TypedPipe]] =
    List(
      // phase 0, add explicit forks to not duplicate pipes on fanout below
      AddExplicitForks,
      // phase 1, compose flatMap/map, move descriptions down, defer merge, filter pushup etc...
      composeSame.orElse(DescribeLater).orElse(FilterKeysEarly).orElse(DeferMerge),
      // phase 2, combine different kinds of mapping operations into flatMaps, including redundant merges
      composeIntoFlatMap.orElse(simplifyEmpty).orElse(DiamondToFlatMap),
      // phase 3, remove duplicates forces/forks (e.g. .fork.fork or .forceToDisk.fork, ....)
      RemoveDuplicateForceFork)
}
