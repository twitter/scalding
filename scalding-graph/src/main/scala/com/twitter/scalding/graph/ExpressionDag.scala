/*
 Copyright 2014 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.scalding.graph

/////////////////////
// There is no logical reason for Literal[T, N] to be here,
// but the scala compiler crashes in 2.9.3 if it is not.
// with:
// java.lang.Error: typeConstructor inapplicable for <none>
//   at scala.tools.nsc.symtab.SymbolTable.abort(SymbolTable.scala:34)
//   at scala.tools.nsc.symtab.Symbols$Symbol.typeConstructor(Symbols.scala:880)
////////////////////

/**
 * This represents literal expressions (no variable redirection)
 * of container nodes of type N[T]
 */
sealed trait Literal[T, N[_]] {
  def evaluate: N[T] = Literal.evaluate(this)
}
case class ConstLit[T, N[_]](override val evaluate: N[T]) extends Literal[T, N]
case class UnaryLit[T1, T2, N[_]](arg: Literal[T1, N],
  fn: N[T1] => N[T2]) extends Literal[T2, N] {
}
case class BinaryLit[T1, T2, T3, N[_]](arg1: Literal[T1, N], arg2: Literal[T2, N],
  fn: (N[T1], N[T2]) => N[T3]) extends Literal[T3, N] {
}

object Literal {
  /**
   * This evaluates a literal formula back to what it represents
   * being careful to handle diamonds by creating referentially
   * equivalent structures (not just structurally equivalent)
   */
  def evaluate[T, N[_]](lit: Literal[T, N]): N[T] =
    evaluate(HMap.empty[({ type L[T] = Literal[T, N] })#L, N], lit)._2

  // Memoized version of the above to handle diamonds
  private def evaluate[T, N[_]](hm: HMap[({ type L[T] = Literal[T, N] })#L, N], lit: Literal[T, N]): (HMap[({ type L[T] = Literal[T, N] })#L, N], N[T]) =
    hm.get(lit) match {
      case Some(prod) => (hm, prod)
      case None =>
        lit match {
          case ConstLit(prod) => (hm + (lit -> prod), prod)
          case UnaryLit(in, fn) =>
            val (h1, p1) = evaluate(hm, in)
            val p2 = fn(p1)
            (h1 + (lit -> p2), p2)
          case BinaryLit(in1, in2, fn) =>
            val (h1, p1) = evaluate(hm, in1)
            val (h2, p2) = evaluate(h1, in2)
            val p3 = fn(p1, p2)
            (h2 + (lit -> p3), p3)
        }
    }
}

sealed trait ExpressionDag[N[_]] { self =>
  // Once we fix N above, we can make E[T] = Expr[T, N]
  type E[t] = Expr[t, N]
  type Lit[t] = Literal[t, N]

  /**
   * These have package visibility to test
   * the law that for all Expr, the node they
   * evaluate to is unique
   */
  protected[graph] def idToExp: HMap[Id, E]
  protected def nodeToLiteral: GenFunction[N, Lit]
  protected def roots: Set[Id[_]]
  protected def nextId: Int

  private def copy(id2Exp: HMap[Id, E] = self.idToExp,
    node2Literal: GenFunction[N, Lit] = self.nodeToLiteral,
    gcroots: Set[Id[_]] = self.roots,
    id: Int = self.nextId): ExpressionDag[N] = new ExpressionDag[N] {
    def idToExp = id2Exp
    def roots = gcroots
    def nodeToLiteral = node2Literal
    def nextId = id
  }

  override def toString: String =
    "ExpressionDag(idToExp = %s)".format(idToExp)

  // This is a cache of Id[T] => Option[N[T]]
  private val idToN =
    new HCache[Id, ({ type ON[T] = Option[N[T]] })#ON]()
  private val nodeToId =
    new HCache[N, ({ type OID[T] = Option[Id[T]] })#OID]()

  /**
   * Add a GC root, or tail in the DAG, that can never be deleted
   * currently, we only support a single root
   */
  private def addRoot[_](id: Id[_]) = copy(gcroots = roots + id)

  /**
   * Which ids are reachable from the roots
   */
  private def reachableIds: Set[Id[_]] = {
    // We actually don't care about the return type of the Set
    // This is a constant function at the type level
    type IdSet[t] = Set[Id[_]]
    def expand(s: Set[Id[_]]): Set[Id[_]] = {
      val partial = new GenPartial[HMap[Id, E]#Pair, IdSet] {
        def apply[T] = {
          case (id, Const(_)) if s(id) => s
          case (id, Var(v)) if s(id) => s + v
          case (id, Unary(id0, _)) if s(id) => s + id0
          case (id, Binary(id0, id1, _)) if s(id) => (s + id0) + id1
        }
      }
      // Note this Stream must always be non-empty as long as roots are
      idToExp.collect[IdSet](partial)
        .reduce(_ ++ _)
    }
    // call expand while we are still growing
    def go(s: Set[Id[_]]): Set[Id[_]] = {
      val step = expand(s)
      if (step == s) s
      else go(step)
    }
    go(roots)
  }

  private def gc: ExpressionDag[N] = {
    val goodIds = reachableIds
    type BoolT[t] = Boolean
    val toKeepI2E = idToExp.filter(new GenFunction[HMap[Id, E]#Pair, BoolT] {
      def apply[T] = { idExp => goodIds(idExp._1) }
    })
    copy(id2Exp = toKeepI2E)
  }

  /**
   * Apply the given rule to the given dag until
   * the graph no longer changes.
   */
  def apply(rule: Rule[N]): ExpressionDag[N] = {
    // for some reason, scala can't optimize this with tailrec
    var prev: ExpressionDag[N] = null
    var curr: ExpressionDag[N] = this
    while (!(curr eq prev)) {
      prev = curr
      curr = curr.applyOnce(rule)
    }
    curr
  }

  protected def toExpr[T](n: N[T]): (ExpressionDag[N], Expr[T, N]) = {
    val (dag, id) = ensure(n)
    val exp = dag.idToExp(id)
    (dag, exp)
  }

  /**
   * Convert a N[T] to a Literal[T, N]
   */
  def toLiteral[T](n: N[T]): Literal[T, N] = nodeToLiteral.apply[T](n)

  /**
   * apply the rule at the first place that satisfies
   * it, and return from there.
   */
  def applyOnce(rule: Rule[N]): ExpressionDag[N] = {
    val getN = new GenPartial[HMap[Id, E]#Pair, HMap[Id, N]#Pair] {
      def apply[U] = {
        val fn = rule.apply[U](self)

        {
          case (id, exp) if fn(exp.evaluate(idToExp)).isDefined =>
            // Sucks to have to call fn, twice, but oh well
            val input = exp.evaluate(idToExp)
            val output = fn(input).get
            (id, output)
        }
      }
    }
    idToExp.collect[HMap[Id, N]#Pair](getN).headOption match {
      case None => this
      case Some(tup) =>
        // some type hand holding
        def act[T](in: HMap[Id, N]#Pair[T]) = {
          val (i, n) = in
          val oldNode = evaluate(i)
          val (dag, exp) = toExpr(n)
          dag.copy(id2Exp = dag.idToExp + (i -> exp))
        }
        // This cast should not be needed
        act(tup.asInstanceOf[HMap[Id, N]#Pair[Any]]).gc
    }
  }

  // This is only called by ensure
  private def addExp[T](node: N[T], exp: Expr[T, N]): (ExpressionDag[N], Id[T]) = {
    val nodeId = Id[T](nextId)
    (copy(id2Exp = idToExp + (nodeId -> exp), id = nextId + 1), nodeId)
  }

  /**
   * This finds the Id[T] in the current graph that is equivalent
   * to the given N[T]
   */
  def find[T](node: N[T]): Option[Id[T]] = nodeToId.getOrElseUpdate(node, {
    val partial = new GenPartial[HMap[Id, E]#Pair, Id] {
      def apply[T] = { case (thisId, expr) if node == expr.evaluate(idToExp) => thisId }
    }
    idToExp.collect(partial).headOption.asInstanceOf[Option[Id[T]]]
  })

  /**
   * This throws if the node is missing, use find if this is not
   * a logic error in your programming. With dependent types we could
   * possibly get this to not compile if it could throw.
   */
  def idOf[T](node: N[T]): Id[T] =
    find(node)
      .getOrElse(sys.error("could not get node: %s\n from %s".format(node, this)))

  /**
   * ensure the given literal node is present in the Dag
   * Note: it is important that at each moment, each node has
   * at most one id in the graph. Put another way, for all
   * Id[T] in the graph evaluate(id) is distinct.
   */
  protected def ensure[T](node: N[T]): (ExpressionDag[N], Id[T]) =
    find(node) match {
      case Some(id) => (this, id)
      case None => {
        val lit: Lit[T] = toLiteral(node)
        lit match {
          case ConstLit(n) =>
            /**
             * Since the code is not performance critical, but correctness critical, and we can't
             * check this property with the typesystem easily, check it here
             */
            assert(n == node,
              "Equality or nodeToLiteral is incorrect: nodeToLit(%s) = ConstLit(%s)".format(node, n))
            addExp(node, Const(n))
          case UnaryLit(prev, fn) =>
            val (exp1, idprev) = ensure(prev.evaluate)
            exp1.addExp(node, Unary(idprev, fn))
          case BinaryLit(n1, n2, fn) =>
            val (exp1, id1) = ensure(n1.evaluate)
            val (exp2, id2) = exp1.ensure(n2.evaluate)
            exp2.addExp(node, Binary(id1, id2, fn))
        }
      }
    }

  /**
   * After applying rules to your Dag, use this method
   * to get the original node type.
   * Only call this on an Id[T] that was generated by
   * this dag or a parent.
   */
  def evaluate[T](id: Id[T]): N[T] =
    evaluateOption(id).getOrElse(sys.error("Could not evaluate: %s\nin %s".format(id, this)))

  def evaluateOption[T](id: Id[T]): Option[N[T]] =
    idToN.getOrElseUpdate(id, {
      val partial = new GenPartial[HMap[Id, E]#Pair, N] {
        def apply[T] = { case (thisId, expr) if (id == thisId) => expr.evaluate(idToExp) }
      }
      idToExp.collect(partial).headOption.asInstanceOf[Option[N[T]]]
    })

  /**
   * Return the number of nodes that depend on the
   * given Id, TODO we might want to cache these.
   * We need to garbage collect nodes that are
   * no longer reachable from the root
   */
  def fanOut(id: Id[_]): Int = {
    // We make a fake IntT[T] which is just Int
    val partial = new GenPartial[E, ({ type IntT[T] = Int })#IntT] {
      def apply[T] = {
        case Var(id1) if (id1 == id) => 1
        case Unary(id1, fn) if (id1 == id) => 1
        case Binary(id1, id2, fn) if (id1 == id) && (id2 == id) => 2
        case Binary(id1, id2, fn) if (id1 == id) || (id2 == id) => 1
        case _ => 0
      }
    }
    idToExp.collectValues[({ type IntT[T] = Int })#IntT](partial).sum
  }

  /**
   * Returns 0 if the node is absent, which is true
   * use .contains(n) to check for containment
   */
  def fanOut(node: N[_]): Int = find(node).map(fanOut(_)).getOrElse(0)
  def contains(node: N[_]): Boolean = find(node).isDefined
}

object ExpressionDag {
  private def empty[N[_]](n2l: GenFunction[N, ({ type L[t] = Literal[t, N] })#L]): ExpressionDag[N] =
    new ExpressionDag[N] {
      val idToExp = HMap.empty[Id, ({ type E[t] = Expr[t, N] })#E]
      val nodeToLiteral = n2l
      val roots = Set.empty[Id[_]]
      val nextId = 0
    }

  /**
   * This creates a new ExpressionDag rooted at the given tail node
   */
  def apply[T, N[_]](n: N[T],
    nodeToLit: GenFunction[N, ({ type L[t] = Literal[t, N] })#L]): (ExpressionDag[N], Id[T]) = {
    val (dag, id) = empty(nodeToLit).ensure(n)
    (dag.addRoot(id), id)
  }

  /**
   * This is the most useful function. Given a N[T] and a way to convert to Literal[T, N],
   * apply the given rule until it no longer applies, and return the N[T] which is
   * equivalent under the given rule
   */
  def applyRule[T, N[_]](n: N[T],
    nodeToLit: GenFunction[N, ({ type L[t] = Literal[t, N] })#L],
    rule: Rule[N]): N[T] = {
    val (dag, id) = apply(n, nodeToLit)
    dag(rule).evaluate(id)
  }
}

/**
 * This implements a simplification rule on ExpressionDags
 */
trait Rule[N[_]] { self =>
  /**
   * If the given Id can be replaced with a simpler expression,
   * return Some(expr) else None.
   *
   * If it is convenient, you might write a partial function
   * and then call .lift to get the correct Function type
   */
  def apply[T](on: ExpressionDag[N]): (N[T] => Option[N[T]])

  // If the current rule cannot apply, then try the argument here
  def orElse(that: Rule[N]): Rule[N] = new Rule[N] {
    def apply[T](on: ExpressionDag[N]) = { n =>
      self.apply(on)(n).orElse(that.apply(on)(n))
    }
  }
}

/**
 * Often a partial function is an easier way to express rules
 */
trait PartialRule[N[_]] extends Rule[N] {
  final def apply[T](on: ExpressionDag[N]) = applyWhere[T](on).lift
  def applyWhere[T](on: ExpressionDag[N]): PartialFunction[N[T], N[T]]
}

