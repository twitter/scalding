package com.twitter.scalding.dagon

import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks._
import org.scalacheck.{Arbitrary, Cogen, Gen}
import scala.util.control.TailCalls

import ScalaVersionCompat.{iterateOnce, lazyListFromIterator, IterableOnce}

object DataFlowTest {
  sealed abstract class Flow[+T] extends Product {
    def filter(fn: T => Boolean): Flow[T] =
      optionMap(Flow.FilterFn(fn))

    def map[U](fn: T => U): Flow[U] =
      optionMap(Flow.MapFn(fn))

    def optionMap[U](fn: T => Option[U]): Flow[U] =
      Flow.OptionMapped(this, fn)

    def concatMap[U](fn: T => IterableOnce[U]): Flow[U] =
      Flow.ConcatMapped(this, fn)

    def ++[U >: T](that: Flow[U]): Flow[U] =
      Flow.Merge(this, that)

    def tagged[A](a: A): Flow[T] =
      Flow.Tagged(this, a)

    /*
     * For large dags you need to eagerly cache hashcode
     * or you will stack overflow
     */
    override val hashCode = scala.util.hashing.MurmurHash3.productHash(this)

    /*
     * You need a custom equals to avoid stack overflow
     */
    override def equals(that: Any) = {
      type Pair = (Flow[Any], Flow[Any])
      import Flow._
      @annotation.tailrec
      def loop(pairs: List[Pair]): Boolean =
        pairs match {
          case Nil => true
          case h :: tail =>
            val (h1, h2) = h
            if (h1 eq h2) loop(tail)
            else
              h match {
                case (IteratorSource(as), IteratorSource(bs)) => (as == bs) && loop(tail)
                case (OptionMapped(f1, fn1), OptionMapped(f2, fn2)) =>
                  (fn1 == fn2) && {
                    val pair = (f1, f2)
                    loop(pair :: tail)
                  }
                case (ConcatMapped(f1, fn1), ConcatMapped(f2, fn2)) =>
                  (fn1 == fn2) && {
                    val pair = (f1, f2)
                    loop(pair :: tail)
                  }
                case (Merge(m1, m2), Merge(m3, m4)) =>
                  val pair1 = (m1, m3)
                  val pair2 = (m2, m4)
                  loop(pair1 :: pair2 :: tail)
                case (Merged(it0), Merged(it1)) =>
                  (it0.size == it1.size) && {
                    val pairs = it0.zip(it1)
                    loop(pairs ::: tail)
                  }
                case (Fork(f1), Fork(f2)) =>
                  val pair = (f1, f2)
                  loop(pair :: tail)
                case (Tagged(f1, a1), Tagged(f2, a2)) =>
                  a1 == a2 && {
                    val pair = (f1, f2)
                    loop(pair :: tail)
                  }
                case (_, _) => false
              }
        }

      that match {
        case f: Flow[_] =>
          // since hashCode is computed, let's use this first
          // after this, we are dealing with collisions and equality
          (this eq f) || ((this.hashCode == f.hashCode) && loop((this, f) :: Nil))
        case _ => false
      }
    }
  }

  object Flow {
    def apply[T](it: Iterator[T]): Flow[T] = IteratorSource(it)

    def dependenciesOf(f: Flow[Any]): List[Flow[Any]] =
      f match {
        case IteratorSource(_)  => Nil
        case OptionMapped(f, _) => f :: Nil
        case ConcatMapped(f, _) => f :: Nil
        case Tagged(f, _)       => f :: Nil
        case Fork(f)            => f :: Nil
        case Merge(left, right) => left :: right :: Nil
        case Merged(ins)        => ins
      }

    def transitiveDeps(f: Flow[Any]): List[Flow[Any]] =
      Graphs.reflexiveTransitiveClosure(List(f))(dependenciesOf _)

    case class IteratorSource[T](it: Iterator[T]) extends Flow[T]
    case class OptionMapped[T, U](input: Flow[T], fn: T => Option[U]) extends Flow[U]
    case class ConcatMapped[T, U](input: Flow[T], fn: T => IterableOnce[U]) extends Flow[U]
    case class Merge[T](left: Flow[T], right: Flow[T]) extends Flow[T]
    case class Merged[T](inputs: List[Flow[T]]) extends Flow[T]
    case class Tagged[A, T](input: Flow[T], tag: A) extends Flow[T]
    case class Fork[T](input: Flow[T]) extends Flow[T]

    def toLiteral: FunctionK[Flow, Literal[Flow, *]] =
      Memoize.functionK[Flow, Literal[Flow, *]](new Memoize.RecursiveK[Flow, Literal[Flow, *]] {
        import Literal._

        def toFunction[T] = {
          case (it @ IteratorSource(_), _)  => Const(it)
          case (o: OptionMapped[s, T], rec) => Unary(rec[s](o.input), { f: Flow[s] => OptionMapped(f, o.fn) })
          case (c: ConcatMapped[s, T], rec) => Unary(rec[s](c.input), { f: Flow[s] => ConcatMapped(f, c.fn) })
          case (t: Tagged[a, s], rec)       => Unary(rec[s](t.input), { f: Flow[s] => Tagged(f, t.tag) })
          case (f: Fork[s], rec)            => Unary(rec[s](f.input), { f: Flow[s] => Fork(f) })
          case (m: Merge[s], rec) =>
            Binary(rec(m.left), rec(m.right), (l: Flow[s], r: Flow[s]) => Merge(l, r))
          case (m: Merged[s], rec) => Variadic(m.inputs.map(rec(_)), { fs: List[Flow[s]] => Merged(fs) })
        }
      })

    def toLiteralTail: FunctionK[Flow, Literal[Flow, *]] =
      FunctionK.andThen[Flow, Lambda[x => TailCalls.TailRec[Literal[Flow, x]]], Literal[Flow, *]](
        Memoize.functionKTailRec[Flow, Literal[Flow, *]](
          new Memoize.RecursiveKTailRec[Flow, Literal[Flow, *]] {
            import Literal._

            def toFunction[T] = {
              case (it @ IteratorSource(_), _) => TailCalls.done(Const(it))
              case (o: OptionMapped[s, T], rec) =>
                rec[s](o.input).map(Unary(_, { f: Flow[s] => OptionMapped(f, o.fn) }))
              case (c: ConcatMapped[s, T], rec) =>
                rec[s](c.input).map(Unary(_, { f: Flow[s] => ConcatMapped(f, c.fn) }))
              case (t: Tagged[a, s], rec) => rec[s](t.input).map(Unary(_, { f: Flow[s] => Tagged(f, t.tag) }))
              case (f: Fork[s], rec)      => rec[s](f.input).map(Unary(_, { f: Flow[s] => Fork(f) }))
              case (m: Merge[s], rec) =>
                for {
                  l <- rec(m.left)
                  r <- rec(m.right)
                } yield Binary(l, r, (l: Flow[s], r: Flow[s]) => Merge(l, r))
              case (m: Merged[s], rec) =>
                def loop(ins: List[Flow[s]]): TailCalls.TailRec[List[Literal[Flow, s]]] =
                  ins match {
                    case Nil => TailCalls.done(Nil)
                    case h :: tail =>
                      for {
                        lh <- rec(h)
                        lt <- loop(ins)
                      } yield lh :: lt
                  }

                loop(m.inputs).map(Variadic(_, { fs: List[Flow[s]] => Merged(fs) }))
            }
          }
        ),
        new FunctionK[Lambda[x => TailCalls.TailRec[Literal[Flow, x]]], Literal[Flow, *]] {
          def toFunction[T] = _.result
        }
      )
    /*
     * use case class functions to preserve equality where possible
     */
    private case class FilterFn[A](fn: A => Boolean) extends Function1[A, Option[A]] {
      def apply(a: A): Option[A] = if (fn(a)) Some(a) else None
    }

    private case class MapFn[A, B](fn: A => B) extends Function1[A, Option[B]] {
      def apply(a: A): Option[B] = Some(fn(a))
    }

    private case class ComposedOM[A, B, C](fn1: A => Option[B], fn2: B => Option[C])
        extends Function1[A, Option[C]] {
      def apply(a: A): Option[C] = {
        // TODO this would be 2x faster if we do it repeatedly and we right associate once in
        // advance
        // this type checks, but can't be tailrec
        // def loop[A1, B1](start: A1, first: A1 => Option[B1], next: B1 => Option[C]): Option[C] =
        @annotation.tailrec
        def loop(start: Any, first: Any => Option[Any], next: Any => Option[C]): Option[C] =
          first match {
            case ComposedOM(f1, f2) =>
              loop(start, f1, ComposedOM(f2, next))
            case notComp =>
              notComp(start) match {
                case None => None
                case Some(b) =>
                  next match {
                    case ComposedOM(f1, f2) =>
                      loop(b, f1, f2)
                    case notComp => notComp(b)
                  }
              }
          }

        loop(a, fn1.asInstanceOf[Any => Option[Any]], fn2.asInstanceOf[Any => Option[C]])
      }
    }
    private case class ComposedCM[A, B, C](fn1: A => IterableOnce[B], fn2: B => IterableOnce[C])
        extends Function1[A, IterableOnce[C]] {
      def apply(a: A): IterableOnce[C] = iterateOnce(fn1(a)).flatMap(fn2)
    }
    private case class OptionToConcatFn[A, B](fn: A => Option[B]) extends Function1[A, IterableOnce[B]] {
      def apply(a: A): IterableOnce[B] = fn(a) match {
        case Some(a) => Iterator.single(a)
        case None    => Iterator.empty
      }
    }

    /**
     * Add explicit fork this is useful if you don't want to have to check each rule for fanout
     *
     * This rule has to be applied from lower down on the graph looking up to avoid cases where Fork(f) exists
     * and f has a fanOut.
     */
    object explicitFork extends Rule[Flow] {
      def needsFork[N[_]](on: Dag[N], n: N[_]): Boolean =
        n match {
          case Fork(_) => false
          case n       => !on.hasSingleDependent(n)
        }
      def apply[T](on: Dag[Flow]) = {
        case OptionMapped(flow, fn) if needsFork(on, flow) =>
          Some(OptionMapped(Fork(flow), fn))
        case ConcatMapped(flow, fn) if needsFork(on, flow) =>
          Some(ConcatMapped(Fork(flow), fn))
        case Tagged(flow, tag) if needsFork(on, flow) =>
          Some(Tagged(Fork(flow), tag))
        case Merge(lhs, rhs) =>
          val (nl, nr) = (needsFork(on, lhs), needsFork(on, lhs))
          if (!nl && !nr) None
          else Some(Merge(if (nl) Fork(lhs) else lhs, if (nr) Fork(rhs) else rhs))
        case Merged(inputs) =>
          val nx = inputs.map(needsFork(on, _))
          if (nx.forall(_ == false)) None
          else Some(Merged(inputs.zip(nx).map { case (n, b) => if (b) Fork(n) else n }))
        case _ =>
          None
      }
    }

    /**
     * f.optionMap(fn1).optionMap(fn2) == f.optionMap { t => fn1(t).flatMap(fn2) } we use object to get good
     * toString for debugging
     */
    object composeOptionMapped extends Rule[Flow] {
      // This recursively scoops up as much as we can into one OptionMapped
      // the Any here is to make tailrec work, which until 2.13 does not allow
      // the types to change on the calls
      @annotation.tailrec
      private def compose[B](
          dag: Dag[Flow],
          flow: Flow[Any],
          fn: Any => Option[B],
          diff: Boolean
      ): (Boolean, OptionMapped[_, B]) =
        flow match {
          case OptionMapped(inner, fn1) if dag.hasSingleDependent(flow) =>
            compose(dag, inner, ComposedOM(fn1, fn), true)
          case _ => (diff, OptionMapped(flow, fn))
        }

      def apply[T](on: Dag[Flow]) = {
        case OptionMapped(inner, fn) =>
          val (changed, res) = compose(on, inner, fn, false)
          if (changed) Some(res)
          else None
        case _ => None
      }
    }

    /**
     * f.concatMap(fn1).concatMap(fn2) == f.concatMap { t => fn1(t).flatMap(fn2) }
     */
    object composeConcatMap extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = {
        case (ConcatMapped(inner @ ConcatMapped(s, fn0), fn1)) if on.hasSingleDependent(inner) =>
          ConcatMapped(s, ComposedCM(fn0, fn1))
      }
    }

    /**
     * (a ++ b).concatMap(fn) == (a.concatMap(fn) ++ b.concatMap(fn)) (a ++ b).optionMap(fn) ==
     * (a.optionMap(fn) ++ b.optionMap(fn))
     */
    object mergePullDown extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = {
        case (ConcatMapped(merge @ Merge(a, b), fn)) if on.hasSingleDependent(merge) =>
          a.concatMap(fn) ++ b.concatMap(fn)
        case (OptionMapped(merge @ Merge(a, b), fn)) if on.hasSingleDependent(merge) =>
          a.optionMap(fn) ++ b.optionMap(fn)
      }
    }

    /**
     * we can convert optionMap to concatMap if we don't care about maintaining the knowledge about which fns
     * potentially expand the size
     */
    object optionMapToConcatMap extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = { case OptionMapped(of, fn) =>
        ConcatMapped(of, OptionToConcatFn(fn))
      }
    }

    /**
     * right associate merges
     */
    object CombineMerges extends Rule[Flow] {
      def apply[T](on: Dag[Flow]) = {
        @annotation.tailrec
        def flatten(f: Flow[T], toCheck: List[Flow[T]], acc: List[Flow[T]]): List[Flow[T]] =
          f match {
            case m @ Merge(a, b) if on.hasSingleDependent(m) =>
              // on the inner merges, we only destroy them if they have no fanout
              flatten(a, b :: toCheck, acc)
            case noSplit =>
              toCheck match {
                case h :: tail => flatten(h, tail, noSplit :: acc)
                case Nil       => (noSplit :: acc).reverse
              }
          }

        { node: Flow[T] =>
          node match {
            case Merge(a, b) =>
              flatten(a, b :: Nil, Nil) match {
                case a1 :: a2 :: Nil =>
                  None // could not simplify
                case many => Some(Merged(many))
              }
            case Merged(list @ (h :: tail)) =>
              val res = flatten(h, tail, Nil)
              if (res != list) Some(Merged(res))
              else None
            case _ => None
          }
        }
      }
    }

    /**
     * evaluate single fanout sources
     */
    object evalSource extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = {
        case OptionMapped(src @ IteratorSource(it), fn) if on.hasSingleDependent(src) =>
          IteratorSource(it.flatMap(fn(_).iterator))
        case ConcatMapped(src @ IteratorSource(it), fn) if on.hasSingleDependent(src) =>
          IteratorSource(it.flatMap(fn))
        case Merge(src1 @ IteratorSource(it1), src2 @ IteratorSource(it2))
            if it1 != it2 && on.hasSingleDependent(src1) && on.hasSingleDependent(src2) =>
          IteratorSource(it1 ++ it2)
        case Merge(src1 @ IteratorSource(it1), src2 @ IteratorSource(it2))
            if it1 == it2 && on.hasSingleDependent(src1) && on.hasSingleDependent(src2) =>
          // we need to materialize the left
          val left = lazyListFromIterator(it1)
          IteratorSource((left ++ left).iterator)
        case Merged(Nil)           => IteratorSource(Iterator.empty)
        case Merged(single :: Nil) => single
        case Merged((src1 @ IteratorSource(it1)) :: (src2 @ IteratorSource(it2)) :: tail)
            if it1 != it2 && on.hasSingleDependent(src1) && on.hasSingleDependent(src2) =>
          Merged(IteratorSource(it1 ++ it2) :: tail)
        case Merged((src1 @ IteratorSource(it1)) :: (src2 @ IteratorSource(it2)) :: tail)
            if it1 == it2 && on.hasSingleDependent(src1) && on.hasSingleDependent(src2) =>
          // we need to materialize the left
          val left = lazyListFromIterator(it1)
          Merged(IteratorSource((left ++ left).iterator) :: tail)
      }
    }

    object removeTag extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = { case Tagged(in, _) =>
        in
      }
    }

    /**
     * these are all optimization rules to simplify
     */
    val allRulesList: List[Rule[Flow]] =
      List(composeOptionMapped, composeConcatMap, mergePullDown, CombineMerges, removeTag, evalSource)

    val allRules = Rule.orElse(allRulesList)

    val ruleGen: Gen[Rule[Flow]] = {
      val allRules = List(
        composeOptionMapped,
        composeConcatMap,
        optionMapToConcatMap,
        mergePullDown,
        CombineMerges,
        evalSource,
        removeTag
      )
      for {
        n <- Gen.choose(0, allRules.size)
        gen = if (n == 0) Gen.const(List(Rule.empty[Flow])) else Gen.pick(n, allRules)
        rs <- gen
      } yield rs.reduce((r1: Rule[Flow], r2: Rule[Flow]) => r1.orElse(r2))
    }

    implicit val arbRule: Arbitrary[Rule[Flow]] =
      Arbitrary(ruleGen)

    def genFlow[T](g: Gen[T])(implicit cogen: Cogen[T]): Gen[Flow[T]] = {
      implicit val arb: Arbitrary[T] = Arbitrary(g)

      def genSource: Gen[Flow[T]] =
        Gen.listOf(g).map(l => Flow(l.iterator))

      /**
       * We want to create DAGs, so we need to sometimes select a parent
       */
      def reachable(f: Flow[T]): Gen[Flow[T]] =
        Gen.lzy(Gen.oneOf(Flow.transitiveDeps(f).asInstanceOf[List[Flow[T]]]))

      val optionMap: Gen[Flow[T]] =
        for {
          parent <- Gen.lzy(genFlow(g))
          fn <- implicitly[Arbitrary[T => Option[T]]].arbitrary
        } yield parent.optionMap(fn)

      val concatMap: Gen[Flow[T]] =
        for {
          parent <- Gen.lzy(genFlow(g))
          fn <- implicitly[Arbitrary[T => List[T]]].arbitrary
        } yield parent.concatMap(fn)

      val merge: Gen[Flow[T]] =
        for {
          left <- Gen.lzy(genFlow(g))
          right <- Gen.frequency((3, genFlow(g)), (2, reachable(left)))
          swap <- Gen.choose(0, 1)
          res = if (swap == 1) (right ++ left) else (left ++ right)
        } yield res

      val tagged: Gen[Flow[T]] =
        for {
          tag <- g
          input <- genFlow(g)
        } yield input.tagged(tag)

      Gen.frequency((4, genSource), (1, optionMap), (1, concatMap), (1, tagged), (1, merge))
    }

    implicit def arbFlow[T: Arbitrary: Cogen]: Arbitrary[Flow[T]] =
      Arbitrary(genFlow[T](implicitly[Arbitrary[T]].arbitrary))

    def expDagGen[T: Cogen](g: Gen[T]): Gen[Dag[Flow]] = {
      val empty = Dag.empty[Flow](toLiteral)

      Gen.frequency((1, Gen.const(empty)), (10, genFlow(g).map(f => empty.addRoot(f)._1)))
    }

    def arbExpDag[T: Arbitrary: Cogen]: Arbitrary[Dag[Flow]] =
      Arbitrary(expDagGen[T](implicitly[Arbitrary[T]].arbitrary))
  }
}

class DataFlowTest extends FunSuite {

  implicit val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 5000)

  import DataFlowTest._

  test("basic test 1") {
    val f1 = Flow((0 to 100).iterator)

    val branch1 = f1.map(_ * 2).filter(_ % 6 != 0)
    val branch2 = f1.map(_ * Int.MaxValue).filter(_ % 6 == 0)

    val tail = (branch1 ++ branch2).map(_ / 3)

    import Flow._

    val res = Dag.applyRule(tail, toLiteral, mergePullDown.orElse(composeOptionMapped))

    res match {
      case Merge(OptionMapped(s1, fn1), OptionMapped(s2, fn2)) =>
        assert(s1 == s2)
      case other => fail(s"$other")
    }
  }

  test("basic test 2") {
    def it1: Iterator[Int] = (0 to 100).iterator
    def it2: Iterator[Int] = (1000 to 2000).iterator

    val f = Flow(it1).map(_ * 2) ++ Flow(it2).filter(_ % 7 == 0)

    Dag.applyRule(f, Flow.toLiteral, Flow.allRules) match {
      case Flow.IteratorSource(it) =>
        assert(it.toList == (it1.map(_ * 2) ++ (it2.filter(_ % 7 == 0))).toList)
      case nonSrc =>
        fail(s"expected total evaluation $nonSrc")
    }

  }

  test("fanOut matches") {

    def law(f: Flow[Int], rule: Rule[Flow], maxApplies: Int) = {
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, maxApplies)
      val optF = optimizedDag.evaluate(id)

      val depGraph = SimpleDag[Flow[Any]](Flow.transitiveDeps(optF))(Flow.dependenciesOf _)

      def fanOut(f: Flow[Any]): Int = {
        val internal = depGraph.fanOut(f).getOrElse(0)
        val external = if (depGraph.isTail(f)) 1 else 0
        internal + external
      }

      optimizedDag.allNodes.foreach { n =>
        assert(depGraph.depth(n) == optimizedDag.depthOf(n), s"$n inside\n$optimizedDag")
        assert(optimizedDag.fanOut(n) == fanOut(n), s"$n in $optimizedDag")
        assert(
          optimizedDag.isRoot(n) == (n == optF),
          s"$n should not be a root, only $optF is, $optimizedDag"
        )
        assert(
          depGraph.isTail(n) == optimizedDag.isRoot(n),
          s"$n is seen as a root, but shouldn't, $optimizedDag"
        )
      }
    }

    forAll(law(_, _, _))

    /**
     * Here we have a list of past regressions
     */
    val it1 = List(1, 2, 3).iterator
    val fn1 = { i: Int => if (i % 2 == 0) Some(i + 1) else None }
    val it2 = List(2, 3, 4).iterator
    val it3 = List(3, 4, 5).iterator
    val fn2 = { i: Int => None }
    val fn3 = { i: Int => (0 to i) }

    import Flow._

    val g = ConcatMapped(
      Merge(
        OptionMapped(IteratorSource(it1), fn1),
        OptionMapped(Merge(IteratorSource(it2), IteratorSource(it3)), fn2)
      ),
      fn3
    )
    law(g, Flow.allRules, 2)
  }

  test("we either totally evaluate or have Iterators with fanOut") {

    def law(f: Flow[Int], ap: Dag[Flow] => Dag[Flow]) = {
      val (dag, id) = Dag(f, Flow.toLiteral)
      val optDag = ap(dag)
      val optF = optDag.evaluate(id)

      optF match {
        case Flow.IteratorSource(_) => succeed
        case nonEval =>
          val depGraph = SimpleDag[Flow[Any]](Flow.transitiveDeps(nonEval))(Flow.dependenciesOf _)

          val fansOut = depGraph.nodes
            .collect { case src @ Flow.IteratorSource(_) =>
              src
            }
            .exists(depGraph.fanOut(_).get > 1)

          assert(fansOut, s"should have fanout: $nonEval")
      }
    }

    forAll(law(_: Flow[Int], dag => dag(Flow.allRules)))
    forAll(law(_: Flow[Int], dag => dag.applySeq(Flow.allRulesList)))
  }

  test("addRoot adds roots") {

    def law[T](d: Dag[Flow], f: Flow[T], p: Boolean) = {
      val (next, id) = d.addRoot(f)
      if (p) {
        println((next, id))
        println(next.evaluate(id))
        println(next.evaluate(id) == f)
        println(next.idOf(f))
      }
      assert(next.isRoot(f))
      assert(next.evaluate(id) == f)
      assert(next.evaluate(next.idOf(f)) == f)
    }

    {
      import Flow._
      val (dag, id0) = Dag(IteratorSource(Iterator.empty), toLiteral)
      val iter0 = IteratorSource(Iterator(0))
      val merged0 = Merge(iter0, iter0)
      val tagged0 = Tagged(merged0, 638667334)
      val merged1 = Merge(iter0, Merge(tagged0, merged0))
      val merged2 = Merge(iter0, merged1)
      assert(merged0 != merged2)
      val tagged1 = Tagged(ConcatMapped(merged2, { i: Int => List(i) }), -2147483648)
      val optMapped0 = OptionMapped(tagged1, { i: Int => Some(i) })
      val flow = Merge(tagged0, optMapped0)

      law(dag, flow, false)
      assert(flow != null)
    }

    implicit val dagArb = Flow.arbExpDag[Int]
    forAll { (d: Dag[Flow], f: Flow[Int]) =>
      law(d, f, false)
    }
  }

  test("all Dag.allNodes agrees with Flow.transitiveDeps") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      val optF = optimizedDag.evaluate(id)
      assert(optimizedDag.allNodes == Flow.transitiveDeps(optF).toSet, s"optimized: $optF $optimizedDag")
    }
  }

  test("transitiveDependenciesOf matches Flow.transitiveDeps") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      val optF = optimizedDag.evaluate(id)
      assert(
        optimizedDag.transitiveDependenciesOf(optF) == (Flow.transitiveDeps(optF).toSet - optF),
        s"optimized: $optF $optimizedDag"
      )
    }
  }

  test("Dag: findAll(n).forall(evaluate(_) == n)") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      optimizedDag.allNodes.foreach { n =>
        optimizedDag.findAll(n).foreach { id =>
          assert(optimizedDag.evaluate(id) == n, s"$id does not eval to $n in $optimizedDag")
        }
      }
    }
  }

  test("apply the empty rule returns eq dag") {
    implicit val dag = Flow.arbExpDag[Int]

    forAll { (d: Dag[Flow]) =>
      assert(d(Rule.empty[Flow]) eq d)
    }
  }

  test("rules are idempotent") {
    def law(f: Flow[Int], rule: Rule[Flow]) = {
      val (dag, id) = Dag(f, Flow.toLiteral)
      val optimizedDag = dag(rule)
      val optF = optimizedDag.evaluate(id)

      val (dag2, id2) = Dag(optF, Flow.toLiteral)
      val optimizedDag2 = dag2(rule)
      val optF2 = optimizedDag2.evaluate(id2)

      assert(optF2 == optF, s"dag1: $optimizedDag -- dag1: $optimizedDag2")
    }

    forAll(law _)
  }

  test("dependentsOf matches SimpleDag.dependantsOf") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)
      val depGraph =
        SimpleDag[Flow[Any]](Flow.transitiveDeps(optimizedDag.evaluate(id)))(Flow.dependenciesOf _)

      optimizedDag.allNodes.foreach { n =>
        assert(depGraph.depth(n) == optimizedDag.depthOf(n))
        assert(
          optimizedDag.dependentsOf(n) == depGraph.dependantsOf(n).fold(Set.empty[Flow[Any]])(_.toSet),
          s"node: $n"
        )
        assert(
          optimizedDag.transitiveDependentsOf(n) ==
            depGraph.transitiveDependantsOf(n).toSet,
          s"node: $n"
        )
      }
    }
  }

  test("dependenciesOf matches toLiteral") {
    forAll { (f: Flow[Int]) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      def contract(n: Flow[_]): List[Flow[_]] =
        Flow.toLiteral(n) match {
          case Literal.Const(_)          => Nil
          case Literal.Unary(n, _)       => n.evaluate :: Nil
          case Literal.Binary(n1, n2, _) => n1.evaluate :: n2.evaluate :: Nil
          case Literal.Variadic(ns, _)   => ns.map(_.evaluate)
        }

      dag.allNodes.foreach { n =>
        assert(dag.dependenciesOf(n) == contract(n))
      }
    }
  }

  test("hasSingleDependent matches fanOut") {
    forAll { (f: Flow[Int]) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      dag.allNodes.foreach { n =>
        assert(dag.hasSingleDependent(n) == (dag.fanOut(n) <= 1))
      }

      dag.allNodes
        .filter(dag.hasSingleDependent)
        .foreach { n =>
          assert(dag.dependentsOf(n).size <= 1)
        }
    }
  }

  test("findAll works as expected") {
    def law(f: Flow[Int], rule: Rule[Flow], max: Int, check: List[Flow[Int]]) = {
      val (dag, _) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      optimizedDag.allNodes.iterator.foreach { n =>
        assert(optimizedDag.findAll(n).nonEmpty, s"findAll: $n $optimizedDag")
      }
      check.filterNot(optimizedDag.allNodes).foreach { n =>
        assert(optimizedDag.findAll(n).isEmpty, s"findAll: $n $optimizedDag")
      }
    }

    law(Flow.IteratorSource(Iterator(1, 2, 3)), Flow.allRules, 1, Nil)
    law(Flow.IteratorSource(Iterator(1, 2, 3)).map(_ * 2), Flow.allRules, 1, Nil)
    law(Flow.IteratorSource(Iterator(1, 2, 3)).map(_ * 2).tagged(100), Flow.allRules, 1, Nil)
    forAll(law _)
  }

  test("contains(n) is the same as allNodes.contains(n)") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int, check: List[Flow[Int]]) =>
      val (dag, _) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      (optimizedDag.allNodes.iterator ++ check.iterator).foreach { n =>
        assert(optimizedDag.contains(n) == optimizedDag.allNodes(n), s"$n $optimizedDag")
      }
    }
  }

  test("all roots can be evaluated") {
    forAll { (roots: List[Flow[Int]], rule: Rule[Flow], max: Int) =>
      val dag = Dag.empty[Flow](Flow.toLiteral)

      // This is pretty slow with tons of roots, take 10
      val (finalDag, allRoots) = roots.take(10).foldLeft((dag, Set.empty[Id[Int]])) { case ((d, s), f) =>
        val (nextDag, id) = d.addRoot(f)
        (nextDag, s + id)
      }

      val optimizedDag = finalDag.applyMax(rule, max)

      allRoots.foreach { id =>
        assert(optimizedDag.evaluateOption(id).isDefined, s"$optimizedDag $id")
      }
    }
  }

  test("removeTag removes all .tagged") {
    forAll { f: Flow[Int] =>
      val (dag, id) = Dag(f, Flow.toLiteral)
      val optDag = dag(Flow.allRules) // includes removeTagged

      optDag.allNodes.foreach {
        case Flow.Tagged(_, _) => fail(s"expected no Tagged, but found one")
        case _                 => succeed
      }
    }
  }

  test("reachableIds are only the set of nodes") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      assert(
        optimizedDag.reachableIds.map(optimizedDag.evaluate(_)) == optimizedDag.allNodes,
        s"$optimizedDag"
      )
    }
  }

  test("adding explicit forks does not loop") {
    forAll { (f: Flow[Int]) =>
      Dag.applyRule(f, Flow.toLiteral, Flow.explicitFork)
    // we are just testing that this does not throw
    }

    // Here are some explicit examples:
    import Flow._
    val src = IteratorSource(Iterator(1))
    val example = ConcatMapped(
      Tagged(Merge(OptionMapped(src, { x: Int => Option(2 * x) }), src), 0),
      { x: Int => List(x) }
    )
    Dag.applyRule(example, Flow.toLiteral, Flow.explicitFork)

    // Here is an example where we have a root that has fanOut
    val d0 = Dag.empty(Flow.toLiteral)
    val (d1, id0) = d0.addRoot(src)
    val (d2, id1) = d1.addRoot(example)

    d2.apply(Flow.explicitFork)
  }

  test("a particular hard case for explicit forks") {
    //
    // Here we have an implicit fork just before an explicit
    // fork, but then try to add explicit forks. This should
    // move the implicit fork down to the explicit fork.
    import Flow._
    val src = IteratorSource(Iterator(1, 2, 3))
    val fn1: Int => Option[Int] = { x => Option(x + 1) }
    val f1 = OptionMapped(src, fn1)
    val f2 = Fork(src)
    val f3 = OptionMapped(f2, { x: Int => Option(x + 2) })
    val f4 = OptionMapped(f2, { x: Int => Option(x + 3) })

    // Here is an example where we have a root that has fanOut
    val d0 = Dag.empty(Flow.toLiteral)
    val (d1, id1) = d0.addRoot(f1)
    val (d2, id3) = d1.addRoot(f3)
    val (d3, id4) = d2.addRoot(f4)

    val d4 = d3.apply(Flow.explicitFork)
    assert(d4.evaluate(id1) == OptionMapped(Fork(src), fn1))
  }

  test("test a giant graph") {
    import Flow._

    @annotation.tailrec
    def incrementChain(f: Flow[Int], incs: Int): Flow[Int] =
      if (incs <= 0) f
      else incrementChain(f.map(_ + 1), incs - 1)

    // val incCount = if (catalysts.Platform.isJvm) 10000 else 1000
    val incCount = 1000

    val incFlow = incrementChain(IteratorSource((0 to 100).iterator), incCount)
    val (dag, id) = Dag(incFlow, Flow.toLiteralTail)

    // make sure we can evaluate the id:
    val node1 = dag.evaluate(id)
    assert(node1 == incFlow)

    assert(dag.depthOfId(id) == Some(incCount))
    assert(dag.depthOf(incFlow) == Some(incCount))

    val optimizedDag = dag(allRules)

    optimizedDag.evaluate(id) match {
      case IteratorSource(it) =>
        assert(it.toList == (0 to 100).map(_ + incCount).toList)
      case other =>
        fail(s"expected to be optimized: $other")
    }
  }
}
