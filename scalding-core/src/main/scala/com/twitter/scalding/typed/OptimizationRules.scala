package com.twitter.scalding.typed

import com.stripe.dagon.{ FunctionK, Memoize, Rule, PartialRule, Dag, Literal }

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
                  // we are relying on the fact that we use Ordering[K]
                  // as a contravariant type, despite it not being defined
                  // that way.
                  CoGroupedPipe(WithReducers(cg, reds))
                case kvPipe =>
                  ReduceStepPipe(IdentityReduce(cg.keyOrdering, kvPipe, None, Nil)
                    .withReducers(reds))
              }
            })
          }
          widen(go(wr))
        case wd@WithDescription(_, _) =>
          def go[V1 <: V](wd: WithDescription[K, V1]): LiteralPipe[(K, V)] = {
            val desc = wd.description
            Unary[TypedPipe, (K, V1), (K, V)](handleCoGrouped(wd.on, recurse), { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  withDescription(rs, desc)
                case CoGroupedPipe(cg) =>
                  // we are relying on the fact that we use Ordering[K]
                  // as a contravariant type, despite it not being defined
                  // that way.
                  CoGroupedPipe(WithDescription(cg, desc))
                case kvPipe =>
                  kvPipe.withDescription(desc)
              }
            })
          }
          widen(go(wd))
        case fk@FilterKeys(_, _) =>
          def go[V1 <: V](fk: FilterKeys[K, V1]): LiteralPipe[(K, V)] = {
            val fn = fk.fn
            Unary[TypedPipe, (K, V1), (K, V)](handleCoGrouped(fk.on, recurse), { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  filterKeys(rs, fn)
                case CoGroupedPipe(cg) =>
                  // we are relying on the fact that we use Ordering[K]
                  // as a contravariant type, despite it not being defined
                  // that way.
                  CoGroupedPipe(FilterKeys(cg, fn))
                case kvPipe =>
                  kvPipe.filterKeys(fn)
              }
            })
          }
          widen(go(fk))
        case mg@MapGroup(_, _) =>
          def go[V1, V2 <: V](mg: MapGroup[K, V1, V2]): LiteralPipe[(K, V)] = {
            val fn = mg.fn
            Unary[TypedPipe, (K, V1), (K, V)](handleCoGrouped(mg.on, recurse), { (tp: TypedPipe[(K, V1)]) =>
              tp match {
                case ReduceStepPipe(rs) =>
                  mapGroup(rs, fn)
                case CoGroupedPipe(cg) =>
                  // we are relying on the fact that we use Ordering[K]
                  // as a contravariant type, despite it not being defined
                  // that way.
                  CoGroupedPipe(MapGroup(cg, fn))
                case kvPipe =>
                  ReduceStepPipe(
                    IdentityReduce(cg.keyOrdering, kvPipe, None, Nil)
                      .mapGroup(fn))
              }
            })
          }
          widen(go(mg))
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
            case ReduceStepPipe(hg: HashJoinable[K, V2]) =>
              HashCoGroup(ltp, hg, joiner)
            case otherwise =>
              HashCoGroup(ltp, IdentityReduce(ordK, otherwise, None, Nil), joiner)
          }
        })
    }
}
