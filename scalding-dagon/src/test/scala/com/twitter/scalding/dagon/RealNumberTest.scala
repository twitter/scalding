package com.twitter.scalding.dagon

import org.scalatest.FunSuite
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.prop.GeneratorDrivenPropertyChecks._

object RealNumbers {

  class SortedList[+A] private (val toList: List[A]) {
    def apply[A1 >: A](a: A1): Boolean =
      toList.contains(a)

    def tail: SortedList[A] =
      new SortedList(toList.tail)

    def filter(fn: A => Boolean): SortedList[A] =
      new SortedList(toList.filter(fn))

    def filterNot(fn: A => Boolean): SortedList[A] =
      new SortedList(toList.filterNot(fn))

    def collect[B: Ordering](fn: PartialFunction[A, B]): SortedList[B] =
      new SortedList(toList.collect(fn).sorted)

    def |[A1 >: A: Ordering](that: SortedList[A1]): SortedList[A1] =
      // could do a merge-sort here in linear time
      new SortedList((toList reverse_::: that.toList).sorted)

    def +[A1 >: A: Ordering](a: A1): SortedList[A1] =
      new SortedList((a :: toList).sorted)

    def removeFirst[A1 >: A](a: A1): SortedList[A1] =
      toList match {
        case Nil => new SortedList(Nil)
        case h :: tail if h == a => new SortedList(tail)
        case h :: tail =>
          val tl = new SortedList(tail)
          new SortedList(h :: (tl.removeFirst(a)).toList)
      }

    def ++[A1 >: A: Ordering](that: Iterable[A1]): SortedList[A1] =
      new SortedList((toList ++ that).sorted)

    override def equals(that: Any) =
      that match {
        case sl: SortedList[_] => toList == sl.toList
        case _ => false
      }
    override def hashCode: Int = toList.hashCode
  }
  object SortedList {
    val empty: SortedList[Nothing] = new SortedList(Nil)

    def unapply[A](list: SortedList[A]): Some[List[A]] =
      Some(list.toList)

    def fromList[A: Ordering](as: List[A]): SortedList[A] =
      new SortedList(as.sorted)

    def apply[A: Ordering](as: A*): SortedList[A] =
      new SortedList(as.toList.sorted)

    implicit def sortedListOrd[A: Ordering]: Ordering[SortedList[A]] = {
      val ordList: Ordering[Iterable[A]] = Ordering.Iterable[A]
      ordList.on { ls: SortedList[A] => ls.toList }
    }

    // Get all the list methods
    implicit def toList[A](sl: SortedList[A]): List[A] = sl.toList
  }

  sealed abstract class Real { self: Product =>
    import Real._

    // cache the hashcode
    override val hashCode = scala.util.hashing.MurmurHash3.productHash(self)
    override def equals(that: Any) = that match {
      case thatF: Real =>
        if (thatF eq this) true
        else if (thatF.hashCode != hashCode) false
        else {
          @annotation.tailrec
          def loop(todo: List[RefPair[Real, Real]], seen: Set[RefPair[Real, Real]]): Boolean =
            todo match {
              case Nil => true
              case rf :: tail =>
                if (rf.itemsEq) loop(tail, seen)
                else rf match {
                  case RefPair(Const(a), Const(b)) =>
                    (a == b) && loop(tail, seen)
                  case RefPair(Variable(a), Variable(b)) =>
                    (a == b) && loop(tail, seen)
                  case RefPair(Sum(as), Sum(bs)) =>
                    (as.size == bs.size) && {
                      val stack =
                        as.iterator.zip(bs.iterator).map { case (a, b) =>
                          RefPair(a, b)
                        }
                        .filterNot(seen)
                        .toList

                      loop(stack reverse_::: tail, seen ++ stack)
                    }
                  case RefPair(Prod(as), Prod(bs)) =>
                    (as.size == bs.size) && {
                      val stack =
                        as.iterator.zip(bs.iterator).map { case (a, b) =>
                          RefPair(a, b)
                        }
                        .filterNot(seen)
                        .toList

                      loop(stack reverse_::: tail, seen ++ stack)
                    }
                  case _ => false
                }
            }
          loop(RefPair[Real, Real](this, thatF) :: Nil, Set.empty)
        }
      case _ => false
    }

    /**
     * This multiplicativily increases the size of the Real,
     * be careful, consider (a1 + a2 + .. an) * (b1 + b2 + .. + bk)
     * we wind up with n*k items from n + k. We can wind up
     * exponentially larger:
     * (a0 + a1)*(b0 + b1)*....
     * will be exponentially larger number of terms after expand
     */
    def expand: Real =
      this match {
        case Const(_) | Variable(_) => this
        case Sum(s) => Real.sum(SortedList.fromList(s.map(_.expand)))
        case Prod(p) =>
          // for all the sums in here we need do the full cross product
          val nonSums = p.filter(_.toSum.isEmpty)
          val sums = p.toList.collect { case Sum(s) => s.toList }
          def cross(ls: List[List[Real]]): List[Real] =
            //(a + b), (c + d)... = a * cross(tail) + b * cross(tail) ...
            ls match {
              case Nil => Const(1.0) :: Nil
              case h0 :: Nil => h0
              case h :: tail =>
                cross(tail).flatMap { term =>
                  h.map { h => Real.prod(SortedList.fromList(h :: term :: Nil)) }
                }
            }
         val sum1 = Real.sum(SortedList.fromList(cross(sums)))
         if (nonSums.isEmpty) sum1
         else {
           Real.prod(Real.prod(nonSums), sum1)
         }
      }

    def +(that: Real): Real =
      Real.sum(this, that)

    def unary_-(): Real =
      Real.prod(Const(-1.0), this)

    def -(that: Real): Real =
      this + (-that)

    def *(that: Real): Real =
      Real.prod(this, that)

    def evaluate(m: Map[String, Double]): Option[Double] =
      this match {
        case Const(d) => Some(d)
        case Variable(v) => m.get(v)
        case Sum(s) =>
          s.iterator.foldLeft(Option(0.0)) {
            case (None, _) => None
            case (Some(d), v) => v.evaluate(m).map(_ + d)
          }
        case Prod(p) =>
          p.iterator.foldLeft(Option(1.0)) {
            case (None, _) => None
            case (Some(d), v) => v.evaluate(m).map(_ * d)
          }
      }

    def freeVars: Set[String] =
      this match {
        case Const(_) => Set.empty
        case Variable(v) => Set(v)
        case Sum(v) => v.iterator.flatMap(_.freeVars).toSet
        case Prod(v) => v.iterator.flatMap(_.freeVars).toSet
      }

    def toSum: Option[Sum] =
      this match {
        case s@Sum(_) => Some(s)
        case _ => None
      }
    def toProd: Option[Prod] =
      this match {
        case p@Prod(_) => Some(p)
        case _ => None
      }
    def toConst: Option[Const] =
      this match {
        case c@Const(_) => Some(c)
        case _ => None
      }

    /**
     * If this divides the current value
     * return a Some(res) such that res * r0 == this
     */
    def divOpt(r0: Real): Option[Real] =
      r0 match {
        case z if z.isDefinitelyZero => None
        case c@Const(_) if !c.isFinite => None
        case same if same == self => Some(one)
        case Prod(ps) =>
          // to divide by a product all must divide
          @annotation.tailrec
          def loop(r: Real, ps: SortedList[Real]): Option[Real] =
            if (ps.isEmpty) Some(r)
            else r.divOpt(ps.head) match {
              case None => None
              case Some(r1) => loop(r1, ps.tail)
            }
          loop(this, ps)
        case nonProd =>
          // note r is not a product, so we can do the naive thing:
          this match {
            case Prod(ps) =>
              // if any of these ps can be divided by r0, we are good
              def allFocii[A](head: List[A], focus: A, tail: List[A], acc: List[(List[A], A, List[A])]): List[(List[A], A, List[A])] =
                tail match {
                  case Nil => (head, focus, Nil) :: acc
                  case h :: t => allFocii(focus :: head, h, t, (head, focus, tail) :: acc)
                }
              ps.toList match {
                case Nil => Const(1.0).divOpt(nonProd)
                case h :: tail =>
                  val trials = allFocii(Nil, h, tail, Nil)
                  trials.iterator.map { case (l, f, r) =>
                    f.divOpt(nonProd).map { div => Real.prod(SortedList.fromList(div :: l reverse_::: r)) }
                  }
                  .collectFirst { case Some(res) => res }
              }
            case c@Const(d) if c.isFinite =>
              nonProd match {
                // we want to make progress, not do a naive division
                case c1@Const(d1) if c1.isFinite && d1 != 1.0 => Some(Const(d/d1))
                case _ => None
              }
            case _ => None
          }
      }

    override def toString = {
      def loop(r: Real): String =
        r match {
          case Variable(x) => x
          case Const(d) => d.toString
          case Sum(s) =>
            s.iterator.map(loop(_)).mkString("(", " + ", ")")
          case Prod(p) =>
            p.iterator.map(loop(_)).mkString("(", "*", ")")
        }
      loop(this)
    }

    def isDefinitelyZero: Boolean =
      this match {
        case Const(d) => d == 0.0
        case Variable(_) => false
        case Sum(s) => s.isEmpty || s.forall(_.isDefinitelyZero)
        case Prod(s) => s.exists(_.isDefinitelyZero)
      }

    def cost: Int = {
      def costOp(s: SortedList[Real]): Int =
        if (s.isEmpty) 0
        else s.iterator.map(_.cost).sum + (s.iterator.length - 1)

      this match {
        case Variable(_) | Const(_) => 0
        case Sum(s) => costOp(s)
        case Prod(p) => costOp(p)
      }
    }

    /**
     * What is the order of polynomial for each variable
     */
    def orderMap: Map[String, Int] =
      this match {
        case Const(_) => Map.empty
        case Variable(x) => Map((x, 1))
        case Sum(items) =>
          items
            .foldLeft(Map.empty[String, Int]) { (o, v) =>
              val ov = v.orderMap
              (o.keySet ++ ov.keySet).foldLeft(o) { case (o, k) =>
                o.updated(k, o.getOrElse(k, 0) max ov.getOrElse(k, 0))
              }
            }
        case Prod(items) =>
          items
            .foldLeft(Map.empty[String, Int]) { (o, v) =>
              val ov = v.orderMap
              (o.keySet ++ ov.keySet).foldLeft(o) { case (o, k) =>
                o.updated(k, o.getOrElse(k, 0) + ov.getOrElse(k, 0))
              }
            }
      }
  }
  object Real {
    case class Const(toDouble: Double) extends Real {
      def isFinite: Boolean =
        java.lang.Double.isFinite(toDouble)
    }
    case class Variable(name: String) extends Real
    // use a sorted set, we have unique representations
    case class Sum(terms: SortedList[Real]) extends Real
    case class Prod(terms: SortedList[Real]) extends Real

    val zero: Real = Const(0.0)
    val one: Real = Const(1.0)

    def const(d: Double): Real = Const(d)
    def variable(v: String): Real = Variable(v)

    // What things can a given number
    def divisors(r: Real): List[Real] =
      (r match {
        case p@Prod(ps) =>
          p :: ps.flatMap(divisors(_))
        case nonProd => nonProd :: Nil
      }).filterNot(_.isDefinitelyZero)

    def sum(a: Real, b: Real): Real =
      sum(SortedList(a, b))

    def sum(s0: SortedList[Real]): Real = {
      val s = s0.filterNot(_.isDefinitelyZero)
      if (s.isEmpty) zero
      else if (s.size == 1) s.head
      else {
        val sums = s.iterator.map(_.toSum).collect { case Some(Sum(ps)) => ps }.toList.flatten
        val nonSum = s.filterNot(_.toSum.isDefined)
        if (sums.isEmpty) Sum(nonSum)
        else sum(nonSum ++ sums)
      }
    }

    def prod(a: Real, b: Real): Real =
      Prod(SortedList(a, b))

    def prod(s0: SortedList[Real]): Real = {
      def isOne(a: Real): Boolean =
        a match {
          case Sum(s) => false
          case Const(d) => d == 1.0
          case Variable(_) => false
          case Prod(s) => s.forall(isOne)
        }

      val s = s0.filterNot(isOne)
      if (s.isEmpty) one
      else if (s.size == 1) s.head
      else if (s.exists(_.isDefinitelyZero)) zero
      else {
        val prods = s.iterator.map(_.toProd).collect { case Some(Prod(ps)) => ps }.toList.flatten
        val nonProd = s.filterNot(_.toProd.isDefined)
        if (prods.isEmpty) Prod(nonProd)
        else prod(nonProd ++ prods)
      }
    }

    implicit def ordReal[R <: Real]: Ordering[R] =
      new Ordering[R] {
        def compareIt(a: Iterator[Real], b: Iterator[Real]): Int = {
          @annotation.tailrec
          def loop(): Int =
            (a.hasNext, b.hasNext) match {
              case (true, true) =>
                val c = compareReal(a.next, b.next)
                if (c == 0) loop() else c
              case (false, true) => -1
              case (true, false) => 1
              case (false, false) => 0
            }

          loop()
        }
        def compare(a: R, b: R) = compareReal(a, b)

        def compareReal(a: Real, b: Real) =
          (a, b) match {
            case (Const(a), Const(b)) => java.lang.Double.compare(a, b)
            case (Const(_), _) => -1
            case (Variable(a), Variable(b)) => a.compareTo(b)
            case (Variable(_), Const(_)) => 1
            case (Variable(_), _) => -1
            case (Sum(a), Sum(b)) => compareIt(a.iterator, b.iterator)
            case (Sum(_), Const(_) | Variable(_)) => 1
            case (Sum(_), Prod(_)) => -1
            case (Prod(a), Prod(b)) => compareIt(a.iterator, b.iterator)
            case (Prod(_), Const(_) | Variable(_) | Sum(_)) => 1
          }
      }

    def genReal(depth: Int): Gen[Real] = {
      val const = Gen.choose(-1000, 1000).map { i => Const(i.toDouble) }
      val variable = Gen.choose('a', 'z').map { v => Variable(v.toString) }
      if (depth <= 0) Gen.oneOf(const, variable)
      else {
        val rec = Gen.lzy(genReal(depth - 1))
        val items = Gen.choose(0, 10).flatMap(Gen.listOfN(_, rec))
        val sum = items.map { ls => Real.sum(SortedList(ls: _*)) }
        val prod = items.map { ls => Real.prod(SortedList(ls: _*)) }
        Gen.oneOf(const, variable, sum, prod)
      }
    }

    implicit val arbReal: Arbitrary[Real] = Arbitrary(genReal(4))

    implicit val shrinkReal: Shrink[Real] =
      Shrink {
        case Const(_) => Stream.empty
        case Variable(_) => Const(1.0) #:: Stream.empty
        case Sum(items) if items.isEmpty => Const(0.0) #:: Stream.empty
        case Sum(items) =>
          val smaller = Sum(items.tail)
          smaller #:: shrinkReal.shrink(smaller)
        case Prod(items) if items.isEmpty => Const(1.0) #:: Stream.empty
        case Prod(items) =>
          val smaller = Prod(items.tail)
          smaller #:: shrinkReal.shrink(smaller)
      }
  }

  type RealN[A] = Real
  def toLiteral: FunctionK[RealN, Literal[RealN, *]] =
    Memoize.functionK[RealN, Literal[RealN, *]](new Memoize.RecursiveK[RealN, Literal[RealN, *]] {
      import Real._
      def toFunction[T] = {
        case (r@(Const(_) | Variable(_)), _) => Literal.Const(r)
        case (Sum(rs), rec) =>
          Literal.Variadic[RealN, T, T](rs.iterator.map(rec[T](_)).toList, { rs => Sum(SortedList(rs: _*)) })
        case (Prod(rs), rec) =>
          Literal.Variadic[RealN, T, T](rs.iterator.map(rec[T](_)).toList, { rs => Prod(SortedList(rs: _*)) })
      }
    })


  sealed trait Parser[+A] {
    def apply(s: String): Option[(String, A)]
    def map[B](fn: A => B): Parser[B] = Parser.Map(this, fn)
    def zip[B](that: Parser[B]): Parser[(A, B)] = Parser.Zip(this, that)
    def |[A1 >: A](that: Parser[A1]): Parser[A1] =
      (this, that) match {
        case (Parser.OneOf(l), Parser.OneOf(r)) => Parser.OneOf(l ::: r)
        case (l, Parser.OneOf(r)) => Parser.OneOf(l :: r)
        case (Parser.OneOf(l), r) => Parser.OneOf(l :+ r)
        case (l, r) => Parser.OneOf(List(l, r))
      }

    def ? : Parser[Option[A]] =
      map(Some(_)) | Parser.Pure(None)

    def *>[B](that: Parser[B]): Parser[B] =
      zip(that).map(_._2)

    def <*[B](that: Parser[B]): Parser[A] =
      zip(that).map(_._1)
  }

  object Parser {
    final case class Pure[A](a: A) extends Parser[A] {
      def apply(s: String) = Some((s, a))
    }
    final case class Map[A, B](p: Parser[A], fn: A => B) extends Parser[B] {
      def apply(s: String) = p(s).map { case (s, a) => (s, fn(a)) }
    }
    final case class Zip[A, B](a: Parser[A], b: Parser[B]) extends Parser[(A, B)] {
      def apply(s: String) = a(s).flatMap { case (s, a) => b(s).map { case (s, b) => (s, (a, b)) } }
    }

    final case class OneOf[A](ls: List[Parser[A]]) extends Parser[A] {
      def apply(s: String) = {
        @annotation.tailrec
        def loop(ls: List[Parser[A]]): Option[(String, A)] =
          ls match {
            case Nil => None
            case h :: tail =>
              h(s) match {
                case None => loop(tail)
                case some => some
              }
          }
        loop(ls)
      }
    }

    final case class Rep[A](a: Parser[A]) extends Parser[List[A]] {
      def apply(str: String) = {
        @annotation.tailrec
        def loop(str: String, acc: List[A]): (String, List[A]) =
          a(str) match {
            case None => (str, acc.reverse)
            case Some((rest, a)) => loop(rest, a :: acc)
          }

        Some(loop(str, Nil))
      }
    }

    final case class StringParser(expect: String) extends Parser[String] {
      val len = expect.length
      def apply(s: String) =
        if (s.startsWith(expect)) Some((s.drop(len), expect))
        else None
    }

    final case class LazyParser[A](p: () => Parser[A]) extends Parser[A] {
      private lazy val pa: Parser[A] = {
        @annotation.tailrec
        def loop(p: Parser[A]): Parser[A] =
          p match {
            case LazyParser(lp) => loop(lp())
            case nonLazy => nonLazy
          }

        loop(p())
      }

      def apply(s: String) = pa(s)
    }

    def str(s: String): Parser[String] = StringParser(s)
    def chr(c: Char): Parser[String] = StringParser(c.toString)
    def number(n: Int): Parser[Int] = StringParser(n.toString).map(_ => n)
    def defer[A](p: => Parser[A]): Parser[A] = LazyParser(() => p)
  }

  val realParser: Parser[Real] = {
    val variable: Parser[Real] =
      Parser.OneOf(
        ('a' to 'z').toList.map(Parser.chr(_))
      ).map(Real.Variable(_))

    val digit: Parser[Int] = Parser.OneOf((0 to 9).toList.map(Parser.number(_)))
    val intP: Parser[Double] =
      digit
        .zip(Parser.Rep(digit))
        .map {
          case (d, ds) =>
            (d :: ds).foldLeft(0.0) { (acc, d) => acc * 10.0 + d }
        }

    val constP =
      (Parser.chr('-').?.zip(intP.zip((Parser.chr('.') *> Parser.Rep(digit)).?)))
        .map {
          case (s, (h, None)) => s.fold(h)(_ => -h)
          case (s, (h, Some(rest))) =>
            val num = rest.reverse.foldLeft(0.0) { (acc, d) => acc / 10.0 + d }
            val pos = h + (num / 10.0)
            s.fold(pos)(_ => -pos)
        }
        .map(Real.const(_))

    val recurse = Parser.defer(realParser)

    def op(str: String): Parser[SortedList[Real]] = {
      val left = Parser.chr('(')
      val right = Parser.chr(')')
      val rest = Parser.Rep(Parser.str(str) *> recurse)
      (left *> recurse.zip(rest) <* right)
        .map {
          case (h, t) => SortedList((h :: t) :_*)
        }
    }

    variable | constP | op(" + ").map(Real.Sum(_)) | op("*").map(Real.Prod(_))
  }

    object CombineProdSum extends Rule[RealN] {
      import Real._

      def apply[A](dag: Dag[RealN]) = {
        case Sum(inner) if inner.exists(_.toSum.isDefined) =>
          val nonSum = inner.filter(_.toSum.isEmpty)
          val innerSums = inner.flatMap(_.toSum match {
            case Some(Sum(s)) => s
            case None => SortedList.empty
          })
          Some(sum(nonSum ++ innerSums))

        case Prod(inner) if inner.exists(_.toProd.isDefined) =>
          val nonProd = inner.filter(_.toProd.isEmpty)
          val innerProds = inner.flatMap(_.toProd match {
            case Some(Prod(s)) => s
            case None => SortedList.empty
          })
          Some(prod(nonProd ++ innerProds))

        case _ => None
      }
    }

    object CombineConst extends Rule[RealN] {
      import Real._

      def combine(r: Real): Option[Real] = r match {
        case Sum(inner) if inner.count(_.toConst.isDefined) > 1 =>
          val nonConst = inner.filter(_.toConst.isEmpty).filterNot(_.isDefinitelyZero)
          val c = inner.collect { case Const(d) => d }.sum
          Some(sum(nonConst + Const(c)))
        case Prod(inner) if inner.count(_.toConst.isDefined) > 1 =>
          val nonConst = inner.filter(_.toConst.isEmpty)
          val c = inner.collect { case Const(d) => d }.product
          Some(prod(nonConst + Const(c)))
        case Prod(inner) if inner.exists(_.isDefinitelyZero) =>
          Some(zero)
        case _ => None
      }

      def apply[A](dag: Dag[RealN]) = combine(_)
    }

    object RemoveNoOp extends Rule[RealN] {
      import Real._
      def apply[A](dag: Dag[RealN]) = {
        case Sum(rs) if rs.exists(_.isDefinitelyZero) =>
          Some(sum(rs.filterNot(_.isDefinitelyZero)))
        case Prod(rs) if rs.collectFirst { case Const(1.0) => () }.nonEmpty =>
          Some(prod(rs))
        case _ => None
      }
    }

    /*
     * The idea here is to take (ab + ac) to a(b + c)
     *
     */
    object ReduceProd extends Rule[RealN] {
      import Real._


      def bestPossible(c0: Int, r: Real): Option[(Int, Real)] =
        r match {
          case Sum(maybeProd) if maybeProd.lengthCompare(1) > 0 =>
            //println(s"trying: $r")
            // these are all the things that divide at least one item
            val allProds = maybeProd.flatMap(divisors(_)).distinct.sorted
            // each of the products are candidates for reducing:
            allProds
              .iterator
              .map { p =>
                // we could try dividing each term by p, but maybe we need to group them
                // into sums to make it work:
                // e.g. (a + b + c + 1*(a + b + c)), here, (a+b+c) does not
                // divide any of the rest, but it does divide the union.
                //
                // To handle this case, if we have a sum, subtract p
                val (hadP, maybeProdNotP) = p match {
                  case Sum(ps) if ps.nonEmpty && ps.forall(maybeProd(_)) =>
                    (true, ps.foldLeft(maybeProd)(_.removeFirst(_)))
                  case _ => (false, maybeProd)
                }
                val divO = maybeProdNotP.toList.map { pr => (pr.divOpt(p), pr) }
                val canDiv = divO.collect { case (Some(res), _) => res }
                val noDiv = divO.collect { case (None, pr) => pr }
                // we don't want to use Real.sum here which can
                // do normalizations we don't want in a rule
                val cd = if (hadP) one :: canDiv else canDiv
                val canDiv1 = Sum(SortedList.fromList(cd))
                val res = (p, canDiv1, noDiv)

                //println(s"$r => $res")
                res
              }
              // we want to factor from at least two items
              .filter {
                case (_, Sum(items), _) => items.lengthCompare(2) >= 0
                case _ => false
              }
              .map { case (p, canDiv1, noDiv) =>
                /*
                 * p*canDiv + noDiv
                 */
                val noDiv1 = sum(SortedList.fromList(noDiv))
                val r1 = sum(prod(p, canDiv1), noDiv1)
                val c1 = r1.cost
                if (c1 < c0) {
                  //println(s"decreased cost: $c1 from $c0: $r => $r1")
                  (c1, r1)
                }
                else {
                  //println(s"did not decrease cost: $c1 from $c0: $r")
                  // this is ad-hoc... other rules can lower cost as well here
                  val canDiv2 = CombineConst.combine(canDiv1).getOrElse(canDiv1)
                  val r1 = sum(prod(p, canDiv2), noDiv1)
                  val res = (r1.cost, r1)
                  //println(s"try 2 to decrease cost: ${res._1} from $c0: $r => $r1")
                  res
                }
              }
              .filter { case (c1, _) => c1 <= c0 } // allow groupings that don't reduce cost
              .toList
              .sorted
              .headOption
          case _ => None
        }

      def apply[A](dag: Dag[RealN]) = { r =>
        bestPossible(r.cost, r).map(_._2)
      }
    }

  val allRules0: Rule[RealN] =
    CombineConst orElse CombineProdSum orElse RemoveNoOp

  // ReduceProd is a bit expensive, do it after everything else can't be applied
  val allRules: List[Rule[RealN]] =
    allRules0 :: (ReduceProd orElse allRules0) :: Nil

  implicit val arbRule: Arbitrary[Rule[RealN]] =
    Arbitrary(Gen.oneOf(CombineConst, CombineProdSum, RemoveNoOp, ReduceProd))

  /**
   * Unsafe string parsing, to be used in testing
   */
  def real(s: String): Real =
    realParser(s) match {
      case None => sys.error(s"couldn't parse: $s")
      case Some(("", r)) => r
      case Some((rest, _)) => sys.error(s"still need to parse: $rest")
    }

  def optimize(r: Real, n: Int): Real = {
    val (dag, id) = Dag[Any, RealN](r, toLiteral)
    val optDag = dag.applyMax(allRules0, n)
    optDag.evaluate(id)
  }

  def optimizeAll(r: Real): Real = {
    val (dag, id) = Dag[Any, RealN](r, toLiteral)
    var seen: Set[Real] = Set(dag.evaluate(id))

    val maxSteps = 1000

    def loop(d: Dag[RealN], max: Int): Dag[RealN] =
      if (max <= 0) {
        println(s"exhausted on $r at ${d.evaluate(id)}")
        d
      }
      else {
        // System.out.print('.')
        // System.out.flush()

        // prefer to use allRules0 until it no longer applies
        val d1 = d.applySeqOnce(allRules)
        val r1 = d1.evaluate(id)
        if (d1 == d) d1
        else if (seen(r1)) {
          // TODO: the rules currently can create loops, :(
          //System.out.println(s"loop (step ${maxSteps - max}): $r from ${d.evaluate(id)} to ${r1}")
          d1
          //loop(d1, max - 1)
        } else {
          seen += r1
          loop(d1, max - 1)
        }
      }
    val optDag = loop(dag, maxSteps)
    val d1 = optDag.evaluate(id)
    d1
  }
}

class RealNumberTest extends FunSuite {
  import RealNumbers._

  implicit val generatorDrivenConfig =
   //PropertyCheckConfiguration(minSuccessful = 5000)
   PropertyCheckConfiguration(minSuccessful = 500)

  def close(a: Double, b: Double, msg: => String) = {
    val diff = Math.abs(a - b)
    if (diff < 1e-6) succeed
    else {
      // this should really only happen for giant numbers
      assert(Math.abs(a) > 1e9 || Math.abs(b) > 1e9, msg)
    }
  }

  def closeOpt(opt: Option[Double], nonOpt: Option[Double], msg: => String) =
    (opt, nonOpt) match {
      case (None, None) => succeed
      case (Some(_), None) =>
        // optimization can make things succeed: 0.0 * a = 0.0, so we don't need to know a
        ()
      case (None, Some(_)) => fail(s"unoptimized succeded: $msg")
      case (Some(a), Some(b)) => close(a, b, s"$msg, $a, $b")
    }


  test("can parse") {
    assert(real("1") == Real.Const(1.0))
    assert(real("1.0") == Real.Const(1.0))
    assert(real("1.5") == Real.Const(1.5))
    assert(real("-1.5") == Real.Const(-1.5))
    assert(real("x") == Real.Variable("x"))
    assert(real("(1 + 2)") == Real.Sum(SortedList(Real.const(1.0), Real.const(2.0))))
    assert(real("(1*2)") == Real.Prod(SortedList(Real.const(1.0), Real.const(2.0))))
  }

  test("combine const") {
    assert(optimizeAll(real("(1 + 2 + 3)")) == real("6"))
  }

  test("we can parse anything") {
    forAll { r: Real =>
      assert(real(r.toString) == r, s"couldn't parse: $r")
    }
  }

  test("optimization reduces cost") {
    def law(r: Real, strong: Boolean, n: Int) = {
      val optR = optimize(r, n)
      assert(r.cost >= optR.cost, s"$r => $optR")
    }
    forAll(Real.genReal(3), Gen.choose(0, 1000))(law(_, false, _))
  }

  test("optimizeAll reduces cost") {
    def law(r: Real, strong: Boolean) = {
      val optR = optimizeAll(r)
      if (strong) assert(r.cost > optR.cost, s"$r => $optR")
      else assert(r.cost >= optR.cost, s"$r => $optR")
    }
    forAll(Real.genReal(3))(law(_, false))

    val strongCases = List(
      "((x*1) + (x*2))",
      "((x*1) + (x*2) + (y*3))",
      "(1 + 2)")

    strongCases.foreach { s => law(real(s), true) }
  }

  test("rules don't loop") {
     def neverLoop(r: Real, rules: List[Rule[RealN]]): Unit = {
       val (dag, id) = Dag[Any, RealN](r, toLiteral)
       var seen: Set[Real] = Set(dag.evaluate(id))
       def loop(d: Dag[RealN]): Unit = {
         //val d1 = d.applySeqOnce(allRules)
         val d1 = d.applySeqOnce(rules)
         val r1 = d1.evaluate(id)
         if (seen(r1)) {
           assert(d1 eq d, s"we have seen: $r1 before. Previous: ${d.evaluate(id)} working on $r")
           ()
         }
         else {
           seen += r1
           loop(d1)
         }
       }

       loop(dag)
     }

     forAll { (r: Real, rules: Set[Rule[RealN]]) =>
       neverLoop(r, rules.toList)
     }
  }

  test("optimization does not change evaluation") {
    def law(r: Real, vars: Map[String, Double], ruleSeq: Seq[Rule[RealN]]) = {
      val optR = Dag.applyRuleSeq[Any, RealN](r, toLiteral, ruleSeq)
      closeOpt(optR.evaluate(vars), r.evaluate(vars), s"$optR, $r")
    }

     val ruleSeqGen: Gen[Seq[Rule[RealN]]] =
       implicitly[Arbitrary[Set[Rule[RealN]]]].arbitrary.map(_.toSeq)

     val genMap = Gen.mapOf(Gen.zip(Gen.choose('a', 'z').map(_.toString), Gen.choose(-1000.0, 1000.0)))

     def genCompleteMap(r: Real): Gen[Map[String, Double]] = {
       val vars = r.freeVars
       val sz = vars.size
       Gen.listOfN(sz, Gen.choose(-1000.0, 1000.0)).map { ds =>
         vars.zip(ds).toMap
       }
     }

     val completeEval =
       Real
         .genReal(3)
         .flatMap { r => genCompleteMap(r).map((r, _)) }

    forAll(Real.genReal(3), genMap, ruleSeqGen)(law(_, _, _))

    forAll(completeEval,
      implicitly[Arbitrary[Set[Rule[RealN]]]].arbitrary) {
        case ((r, m), rules) =>
          val res0 = r.evaluate(m)
          val rOpt = Dag.applyRuleSeq[Any, RealN](r, toLiteral, rules.toList)
          val resOpt = rOpt.evaluate(m)

          // scalacheck's broken shrinking kills this, if you see
          // fishy failures, comment it out, until you debug
          assert(res0.isDefined, s"expected unoptimized to work")
          assert(resOpt.isDefined, s"expected optimized to work")
          closeOpt(resOpt, res0, s"$rOpt, $r")
      }

    forAll(completeEval, Real.genReal(3)) { case ((r0, vars), r1) =>
      r0.divOpt(r1) match {
        case None => ()
        case Some(r2) =>
          // r0/r1 == r2, so r0 == r1 * r2
          closeOpt(Real.prod(r1, r2).evaluate(vars), r0.evaluate(vars), s"numerator: $r2")
      }
    }

    // past failures here
     List(
        ("((-986.0*x) + (-325.0*363.0*z) + (530.0*928.0*x))",
          Map("x" -> 1.0, "z" -> 10.0), allRules0 :: Nil),
        ("(q + q + r + x + (-755.0*-394.0*674.0))",
          Map("x" -> 0.0, "q" -> 0.0, "r" -> 3.3953184045350335E-5), ReduceProd :: Nil),
        ("(x + (y + ((x*2) + (y*2))))", Map("x" -> 1.0, "y" -> 10.0), allRules)
      ).foreach {case (inputS, vars, ruleSet) => law(real(inputS), vars, ruleSet) }

  }

  test("evaluation works when all vars are present") {
    val genMap = Gen.mapOf(Gen.zip(Gen.choose('a', 'z').map(_.toString), Gen.choose(-1000.0, 1000.0)))
    forAll(Real.genReal(5), genMap) { (r, vars) =>
      r.evaluate(vars) match {
        case None => assert((r.freeVars -- vars.keys).nonEmpty)
        case Some(_) => assert((r.freeVars -- vars.keys).isEmpty)
      }
    }
  }

  test("cost is expected") {
    List(
      ("(x + (1 + 2))", 2),
      ("(x + (1*2))", 2),
      ("(2.0*(2.0*2.0))", 2),
      ("x", 0),
      ("((a + b)*(c + d))", 3),
      ("((a*c) + (b*c) + (a*d) + (b*d))", 7),
      ("(4*2)", 1)
    ).foreach { case (inputS, expCost) =>
      val input = real(inputS)
      assert(input.cost == expCost, s"$inputS -> $input has cost: ${input.cost} not $expCost")
    }
  }

   test("we fold in constants") {
     List(
       ("(x + x)", "(2*x)"),
       ("(x + (1 + 2))", "(x + 3)"),
       ("(x + (1*2))", "(x + 2)"),
       ("(2.0*(2.0*2.0))", "8.0"),
       ("(1*2)", "2")
     ).foreach { case (input, exp) =>
       val opt = optimizeAll(real(input))
       assert(opt == real(exp), s"$input => $opt not $exp")
     }
   }

   test("expand works as expected") {
     val genMap = Gen.mapOf(Gen.zip(Gen.choose('a', 'z').map(_.toString), Gen.choose(-1000.0, 1000.0)))

     // expand can exponentially increase the size of the real so we can't make it too big
     forAll(Real.genReal(2), genMap)  { (r, vars) =>
       val rexp = r.expand
       closeOpt(rexp.evaluate(vars), r.evaluate(vars), s"$rexp, $r")
     }

     assert(real("((a + b)*(c + d))").expand == real("((a*c) + (a*d) + (b*c) + (b*d))"))
   }


   test("Real.divOpt and divisors agree") {
     forAll { r: Real =>
       Real.divisors(r).foreach { div =>
         r.divOpt(div) match {
           case None => fail(s"expected to divide: $r with $div")
           case Some(_) => succeed
         }

         r.divOpt(r) match {
           case None => fail(s"we expect a self division: $r")
           case Some(_) => succeed
         }
       }
     }
   }

   test("optimization de-foils easy cases") {
     val var1 = Gen.choose('a', 'm').map { c => Real.Variable(c.toString) }
     val var2 = Gen.choose('n', 'z').map { c => Real.Variable(c.toString) }
     def sumOf(g: Gen[Real]) =
       Gen.choose(2, 4).flatMap(Gen.listOfN(_, g))
         .map { ls => Real.sum(SortedList(ls: _*)) }

     val s1 = sumOf(var1)
     val s2 = sumOf(var2)
     val prod = Gen.zip(s1, s2).map { case (a, b) => Real.prod(a, b) }

     // these products of sums
     forAll(prod) { r: Real =>
       val cost0 = r.cost
       // expanded has exponentially more cost than r
       val expanded = r.expand
       val expCost = expanded.cost
       val optR = optimizeAll(expanded)
       val (dag, id) = Dag[Any, RealN](optR, toLiteral)

       // we cannot apply the ReduceProd rule:
       assert(dag.applyOnce(ReduceProd) == dag)

       assert(optR.cost <= cost0, s"$r ($cost0) optimized to $optR expanded to: $expanded expanded cost: $expCost")
     }
   }

   test("optimization nearly de-foils") {
     val sum = Gen.choose(2, 4).flatMap(Gen.listOfN(_, Real.genReal(0)))
       .map { ls => Real.sum(SortedList(ls: _*)) }
     val prod = Gen.choose(2, 3).flatMap(Gen.listOfN(_, sum))
       .map { ls => Real.prod(SortedList(ls: _*)) }
     // these products of sums
     forAll(prod) { r: Real =>
       val cost0 = r.cost
       // expanded has exponentially more cost than r
       val expanded = r.expand
       val expCost = expanded.cost
       // we should get close to minimal, which is cost0:
       val closeness = 0.5 // 1.0 means all the way to minimal
       val prettyGood = cost0 * closeness + expCost.toDouble * (1.0 - closeness)
       val optR = optimizeAll(expanded)
       assert(optR.cost.toDouble <= prettyGood, s"$r ($cost0) optimized to $optR expanded to: $expanded expanded cost: $expCost")
     }
   }

   test("orderMap works") {
     List(
       ("x", Map("x" -> 1)),
       ("(x*x)", Map("x" -> 2)),
       ("((x + 1)*(x + 2))", Map("x" -> 2)),
       ("((x + x)*(x + 2))", Map("x" -> 2)))
       .foreach { case (r, o) =>
         assert(real(r).orderMap == o, s"$r")
       }
   }

   test("test L2 norm example") {
     import Real._
     val terms = 100
     val points = (1 to terms).map { i => const(i.toDouble) }
     // sum_i (d_i - x)*(d_i - x)
     val x = Variable("x")
     val l2 = Real.sum(SortedList.fromList(points.map { d => (d - x) * (d - x) }.toList))

     object ExpandWhenOrderMatches extends Rule[RealN] {
       def apply[T](on: Dag[RealN]) = {
         case s@Sum(items) =>
           val newItems = items
             .groupBy(_.orderMap)
             .iterator
             .map { case (_, vs) =>
               // we can combine sums of the same order:
               if (vs.size > 1) {
                 sum(SortedList.fromList(vs.iterator.map(_.expand).toList))
               }
               else vs.head
             }
             .toList

           val newSum = sum(SortedList.fromList(newItems))
           if (items == newSum) None
           else Some(newSum)
         case _ => None
       }
     }

     val optL2 = Dag.applyRuleSeq[Any, RealN](l2, toLiteral, ExpandWhenOrderMatches :: allRules)

     val optC = optL2.cost
     assert(optC == 4) // we can convert polynomial order 2 to a*(b + x*(c + x))
   }
}
