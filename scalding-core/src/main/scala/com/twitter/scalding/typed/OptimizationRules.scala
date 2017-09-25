package com.twitter.scalding.typed

import com.stripe.dagon.{ FunctionK, Memoize, Rule, PartialRule, Dag, Literal }

object OptimizationRules {
  type LitPipe[T] = Literal[TypedPipe, T]

  import Literal.{ Unary, Binary }
  import TypedPipe._

  /**
   * Since our TypedPipe is covariant, but the Literal is not
   * this is actually safe in this context, but not in general
   */
  def widen[T](l: LitPipe[_ <: T]): LitPipe[T] = {
    // to prove this is safe, see that if you have
    // LitPipe[_ <: T] we can call .evaluate to get
    // TypedPipe[_ <: T] which due to covariance is
    // TypedPipe[T], and then using toLiteral we can get
    // LitPipe[T]
    //
    // that would be wasteful to apply since the final
    // result is identity.
    l.asInstanceOf[LitPipe[T]]
  }

  /**
   * Convert a TypedPipe[T] to a Literal[TypedPipe, T] for
   * use with Dagon
   */
  def toLiteral: FunctionK[TypedPipe, LitPipe] =
    Memoize.functionK[TypedPipe, LitPipe](
      new Memoize.RecursiveK[TypedPipe, LitPipe] {

        def toFunction[A] = {
          case (c: CrossPipe[a, b], f) =>
            Binary(f(c.left), f(c.right), CrossPipe(_: TypedPipe[a], _: TypedPipe[b]))
          case (cv@CrossValue(_, _), f) =>
            def go[A, B](cv: CrossValue[A, B]): LitPipe[(A, B)] =
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

    private def handleReduceStep[K, V1, V2](rs: ReduceStep[K, V1, V2], recurse: FunctionK[TypedPipe, LitPipe]): LitPipe[(K, V2)] =
      rs match {
        case step@IdentityReduce(_, _, _, _) =>
          Unary(widen[(K, V2)](recurse(step.mapped)), { (tp: TypedPipe[(K, V2)]) => ReduceStepPipe(step.copy(mapped = tp)) })
        case step@UnsortedIdentityReduce(_, _, _, _) =>
          Unary(widen[(K, V2)](recurse(step.mapped)), { (tp: TypedPipe[(K, V2)]) => ReduceStepPipe(step.copy(mapped = tp)) })
        case step@IdentityValueSortedReduce(_, _, _, _, _) =>
          def go[A, B](ivsr: IdentityValueSortedReduce[A, B]): LitPipe[(A, B)] =
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
          def go[A, B, C](vsr: ValueSortedReduce[A, B, C]): LitPipe[(A, C)] =
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
          def go[A, B, C](imr: IteratorMappedReduce[A, B, C]): LitPipe[(A, C)] =
            Unary(recurse(imr.mapped), { (tp: TypedPipe[(A, B)]) => ReduceStepPipe[A, B, C](imr.copy(mapped = tp)) })

          go(step)
      }

    private def handleCoGrouped[K, V](cg: CoGroupable[K, V], recurse: FunctionK[TypedPipe, LitPipe]): LitPipe[(K, V)] = {
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
          def go[A, B, C](pair: Pair[K, A, B, C]): LitPipe[(K, C)] = {
            val llit = handleCoGrouped(pair.larger, recurse)
            val rlit = handleCoGrouped(pair.smaller, recurse)
            val fn = pair.fn
            Binary(llit, rlit, { (l: TypedPipe[(K, A)], r: TypedPipe[(K, B)]) =>
              Pair(pipeToCG(l), pipeToCG(r), fn)
            })
          }
          widen(go(p))
        case wr@WithReducers(_, _) =>
          def go[V1 <: V](wr: WithReducers[K, V1]): LitPipe[(K, V)] = {
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
          def go[V1 <: V](wd: WithDescription[K, V1]): LitPipe[(K, V)] = {
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
          def go[V1 <: V](fk: FilterKeys[K, V1]): LitPipe[(K, V)] = {
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
          def go[V1, V2 <: V](mg: MapGroup[K, V1, V2]): LitPipe[(K, V)] = {
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

    private def handleHashCoGroup[K, V, V2, R](hj: HashCoGroup[K, V, V2, R], recurse: FunctionK[TypedPipe, LitPipe]): LitPipe[(K, R)] = {
      val rightLit: LitPipe[(K, V2)] = hj.right match {
        case step@IdentityReduce(_, _, _, _) =>
          Unary(widen[(K, V2)](recurse(step.mapped)), { (tp: TypedPipe[(K, V2)]) => ReduceStepPipe(step.copy(mapped = tp)) })
        case step@UnsortedIdentityReduce(_, _, _, _) =>
          Unary(widen[(K, V2)](recurse(step.mapped)), { (tp: TypedPipe[(K, V2)]) => ReduceStepPipe(step.copy(mapped = tp)) })
        case step@IteratorMappedReduce(_, _, _, _, _) =>
          def go[A, B, C](imr: IteratorMappedReduce[A, B, C]): LitPipe[(A, C)] =
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


  /////////////////////////////
  //
  // Here are some actual rules for simplifying TypedPipes
  //
  /////////////////////////////

  object ComposeFlatMap extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case FlatMapped(FlatMapped(in, fn0), fn1) =>
        FlatMapped(in, FlatMappedFn(fn1).runAfter(FlatMapping.FlatM(fn0)))
      case FlatMapValues(FlatMapValues(in, fn0), fn1) =>
        FlatMapValues(in, FlatMappedFn(fn1).runAfter(FlatMapping.FlatM(fn0)))
    }
  }

  object ComposeMap extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case Mapped(Mapped(in, fn0), fn1) =>
        Mapped(in, ComposedMapFn(fn0, fn1))
      case MapValues(MapValues(in, fn0), fn1) =>
        MapValues(in, ComposedMapFn(fn0, fn1))
    }
  }

  object ComposeFilter extends Rule[TypedPipe] {
    def apply[T](on: Dag[TypedPipe]) = {
      // scala can't type check this, so we hold its hand:
      // case Filter(Filter(in, fn0), fn1) =>
      //   Some(Filter(in, ComposedFilterFn(fn0, fn1)))
      case f@Filter(_, _) =>
        def go[A](f: Filter[A]): Option[TypedPipe[A]] =
          f.input match {
            case f1: Filter[a] =>
              Some(Filter[a](f1.input, ComposedFilterFn(f.fn, f.fn)))
            case _ => None
          }
        go(f)
      case FilterKeys(FilterKeys(in, fn0), fn1) =>
        Some(FilterKeys(in, ComposedFilterFn(fn0, fn1)))
      case _ => None
    }
  }

  object ComposeWithOnComplete extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case WithOnComplete(WithOnComplete(pipe, fn0), fn1) =>
        WithOnComplete(pipe, ComposedOnComplete(fn0, fn1))
    }
  }

  object RemoveDuplicateForceFork extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case ForceToDisk(ForceToDisk(t)) => ForceToDisk(t)
      case ForceToDisk(Fork(t)) => ForceToDisk(t)
      case Fork(Fork(t)) => Fork(t)
      case Fork(ForceToDisk(t)) => ForceToDisk(t)
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
    def handleFilter[A]: PartialFunction[Filter[A], TypedPipe[A]] = {
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
   * This is an optimization we didn't do in scalding 0.17 and earlier
   * because .toTypedPipe on the group totally hid the structure from
   * us
   */
  object FilterKeysEarly extends Rule[TypedPipe] {
    def apply[T](on: Dag[TypedPipe]) = {
      case FilterKeys(ReduceStepPipe(rsp), fn) =>
        Some(rsp match {
          case step@IdentityReduce(_, _, _, _) =>
            ReduceStepPipe(step.filterKeys(fn))
          case step@UnsortedIdentityReduce(_, _, _, _) =>
            ReduceStepPipe(step.filterKeys(fn))
          case step@IdentityValueSortedReduce(_, _, _, _, _) =>
            ReduceStepPipe(step.filterKeys(fn))
          case step@ValueSortedReduce(_, _, _, _, _, _) =>
            ReduceStepPipe(step.filterKeys(fn))
          case step@IteratorMappedReduce(_, _, _, _, _) =>
            ReduceStepPipe(step.filterKeys(fn))
        })
      case FilterKeys(CoGroupedPipe(cg), fn) =>
        Some(CoGroupedPipe(CoGrouped.FilterKeys(cg, fn)))
      case FilterKeys(HashCoGroup(left, right, joiner), fn) =>
        val newRight = right match {
          case step@IdentityReduce(_, _, _, _) => step.filterKeys(fn)
          case step@UnsortedIdentityReduce(_, _, _, _) => step.filterKeys(fn)
          case step@IteratorMappedReduce(_, _, _, _, _) => step.filterKeys(fn)
        }
        Some(HashCoGroup(FilterKeys(left, fn), newRight, joiner))
      case _ => None
    }
  }

  object EmptyIsOftenNoOp extends PartialRule[TypedPipe] {

    def emptyCogroup[K, V](cg: CoGrouped[K, V]): Boolean = {
      import CoGrouped._

      def empty(t: TypedPipe[Any]): Boolean = t match {
        case EmptyTypedPipe => true
        case _ => false
      }
      cg match {
        case Pair(left, right, _) if left.inputs.forall(empty) && right.inputs.forall(empty) => true
        case Pair(left, _, fn) if left.inputs.forall(empty) && (fn eq Joiner.inner2)  => true
        case Pair(_, right, fn) if right.inputs.forall(empty) && (fn eq Joiner.inner2)  => true
        case _ => false
      }
    }

    def applyWhere[T](on: Dag[TypedPipe]) = {
      case CrossPipe(EmptyTypedPipe, _) => EmptyTypedPipe
      case CrossPipe(_, EmptyTypedPipe) => EmptyTypedPipe
      case CrossValue(EmptyTypedPipe, _) => EmptyTypedPipe
      case DebugPipe(EmptyTypedPipe) => EmptyTypedPipe
      case FilterKeys(EmptyTypedPipe, _) => EmptyTypedPipe
      case Filter(EmptyTypedPipe, _) => EmptyTypedPipe
      case FlatMapValues(EmptyTypedPipe, _) => EmptyTypedPipe
      case FlatMapped(EmptyTypedPipe, _) => EmptyTypedPipe
      case ForceToDisk(EmptyTypedPipe) => EmptyTypedPipe
      case Fork(EmptyTypedPipe) => EmptyTypedPipe
      case HashCoGroup(EmptyTypedPipe, _, _) => EmptyTypedPipe
      case MapValues(EmptyTypedPipe, _) => EmptyTypedPipe
      case Mapped(EmptyTypedPipe, _) => EmptyTypedPipe
      case MergedTypedPipe(EmptyTypedPipe, a) => a
      case MergedTypedPipe(a, EmptyTypedPipe) => a
      case ReduceStepPipe(rs: ReduceStep[_, _, _]) if rs.mapped == EmptyTypedPipe => EmptyTypedPipe
      case SumByLocalKeys(EmptyTypedPipe, _) => EmptyTypedPipe
      case CoGroupedPipe(cgp) if emptyCogroup(cgp) => EmptyTypedPipe
    }
  }

  object EmptyIterableIsEmpty extends PartialRule[TypedPipe] {
    def applyWhere[T](on: Dag[TypedPipe]) = {
      case IterablePipe(it) if it.isEmpty => EmptyTypedPipe
    }
  }

  /**
   * To keep equality for case matching and caching, we need to create internal case classes
   */
  private[this] case class ComposedMapFn[A, B, C](fn0: A => B, fn1: B => C) extends Function1[A, C] {
    def apply(a: A) = fn1(fn0(a))
  }
  private[this] case class ComposedFilterFn[-A](fn0: A => Boolean, fn1: A => Boolean) extends Function1[A, Boolean] {
    def apply(a: A) = fn0(a) && fn1(a)
  }
  private[this] case class ComposedOnComplete(fn0: () => Unit, fn1: () => Unit) extends Function0[Unit] {
    def apply(): Unit = {
      @annotation.tailrec
      def loop(fn: () => Unit, stack: List[() => Unit]): Unit =
        fn match {
          case ComposedOnComplete(left, right) => loop(left, right :: stack)
          case notComposed =>
            notComposed()
            stack match {
              case h :: tail => loop(h, tail)
              case Nil => ()
            }
        }

      loop(fn0, List(fn1))
    }
  }

}
