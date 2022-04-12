/*
 Copyright 2014 Twitter, Inc.
 Copyright 2017 Stripe, Inc.

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

package com.twitter.scalding.dagon

import com.twitter.scalding.dagon.ScalaVersionCompat.{lazyListToIterator, LazyList}

import java.io.Serializable
import scala.util.control.TailCalls

/**
 * Represents a directed acyclic graph (DAG).
 *
 * The type N[_] represents the type of nodes in the graph.
 */
sealed abstract class Dag[N[_]] extends Serializable { self =>

  /**
   * These have package visibility to test the law that for all Expr, the node they evaluate to is unique
   */
  protected def idToExp: HMap[Id, Expr[N, *]]

  /**
   * The set of roots that were added by addRoot. These are Ids that will always evaluate such that
   * roots.forall(evaluateOption(_).isDefined)
   */
  protected def roots: Set[Id[_]]

  /**
   * Convert a N[T] to a Literal[T, N].
   */
  def toLiteral: FunctionK[N, Literal[N, *]]

  // Caches polymorphic functions of type Id[T] => Option[N[T]]
  private val idToN: HCache[Id, Lambda[t => Option[N[t]]]] =
    HCache.empty[Id, Lambda[t => Option[N[t]]]]

  // Caches polymorphic functions of type Literal[N, T] => Option[Id[T]]
  private val litToId: HCache[Literal[N, *], Lambda[t => Option[Id[t]]]] =
    HCache.empty[Literal[N, *], Lambda[t => Option[Id[t]]]]

  // Caches polymorphic functions of type Expr[N, T] => N[T]
  private val evalMemo = Expr.evaluateMemo(idToExp)

  /**
   * String representation of this DAG.
   */
  override def toString: String =
    s"Dag(idToExp = $idToExp, roots = $roots)"

  /**
   * Which ids are reachable from the roots?
   */
  def reachableIds: Set[Id[_]] =
    rootsUp.map(_._2).toSet

  /**
   * Apply the given rule to the given dag until the graph no longer changes.
   */
  def apply(rule: Rule[N]): Dag[N] = {

    @annotation.tailrec
    def loop(d: Dag[N]): Dag[N] = {
      val next = d.applyOnce(rule)
      if (next eq d) next
      else loop(next)
    }

    loop(this)
  }

  /**
   * Apply a sequence of rules, which you may think of as phases, in order First apply one rule until it does
   * not apply, then the next, etc..
   */
  def applySeq(phases: Seq[Rule[N]]): Dag[N] =
    phases.foldLeft(this)((dag, rule) => dag(rule))

  def applySeqOnce(phases: Seq[Rule[N]]): Dag[N] =
    phases.iterator
      .map(rule => applyOnce(rule))
      .filter(_ ne this)
      .take(1)
      .toList
      .headOption
      .getOrElse(this)

  /**
   * apply the rule at the first place that satisfies it, and return from there.
   */
  def applyOnce(rule: Rule[N]): Dag[N] = {
    type DagT[T] = Dag[N]

    val f = new FunctionK[HMap[Id, Expr[N, *]]#Pair, Lambda[x => Option[DagT[x]]]] {
      def toFunction[U] = { (kv: (Id[U], Expr[N, U])) =>
        val (id, expr) = kv

        if (expr.isVar) None // Vars always point somewhere, apply the rule there
        else {
          val n1 = evaluate(id)
          rule
            .apply[U](self)(n1)
            .filter(_ != n1)
            .map { n2 =>
              // A node can have several Ids.
              // we need to point ALL of the old ids to the new one
              val oldIds =
                findAll(n1) match {
                  case absent if absent.isEmpty =>
                    sys.error(s"unreachable code, $n1 should have id $id")
                  case existing => existing
                }
              // If n2 depends on n1, the Var trick fails and introduces
              // loops. To avoid this, we have to work in an edge based
              // approach. For all n3 != n2, if they depend on n1, replace
              // with n2. Leave n2 alone.

              // Get an ID for the new node
              // if this new node points to the old node
              // we are going to create a cycle, since
              // below we point the old nodes back to the
              // new id. To fix this, re-reassign
              // n1 to a new id, since that new id won't be
              // updated to point to itself, we prevent a loop
              val newIdN1 = Id.next[U]()
              val dag1 = replaceId(newIdN1, expr, n1)
              val (dag2, newId) = dag1.ensure(n2)

              // We can't delete Ids which may have been shared
              // publicly, and the ids may be embedded in many
              // nodes. Instead we remap 'ids' to be a pointer
              // to 'newid'.
              dag2.repointIds(n1, oldIds, newId, n2)
            }
        }
      }
    }

    // We want to apply rules
    // in a deterministic order so they are reproducible
    rootsUp
      .map { case (_, id) =>
        // use the method to fix the types below
        // if we don't use DagT here, scala thinks
        // it is unused even though we use it above
        def go[A](id: Id[A]): Option[DagT[A]] = {
          val expr = idToExp(id)
          f.toFunction[A]((id, expr))
        }
        go(id)
      }
      .collectFirst { case Some(dag) => dag }
      .getOrElse(this)
  }

  /**
   * Apply a rule at most cnt times.
   */
  def applyMax(rule: Rule[N], cnt: Int): Dag[N] = {

    @annotation.tailrec
    def loop(d: Dag[N], cnt: Int): Dag[N] =
      if (cnt <= 0) d
      else {
        val next = d.applyOnce(rule)
        if (next eq d) d
        else loop(next, cnt - 1)
      }

    loop(this, cnt)
  }

  def depthOfId[A](i: Id[A]): Option[Int] =
    depth.get(i)

  def depthOf[A](n: N[A]): Option[Int] =
    find(n).flatMap(depthOfId(_))

  private lazy val depth: Map[Id[_], Int] = {
    sealed trait Rest {
      def dependsOn(id: Id[_]): Boolean
    }
    sealed case class Same(asId: Id[_]) extends Rest {
      def dependsOn(id: Id[_]) = id == asId
    }
    sealed case class MaxInc(a: Id[_], b: Id[_]) extends Rest {
      def dependsOn(id: Id[_]) = (id == a) || (id == b)
    }
    sealed case class Inc(of: Id[_]) extends Rest {
      def dependsOn(id: Id[_]) = id == of
    }
    sealed case class Variadic(ids: List[Id[_]]) extends Rest {
      def dependsOn(id: Id[_]) = ids.contains(id)
    }

    @annotation.tailrec
    def lookup(
        state: Map[Id[_], Int],
        todo: List[(Id[_], Rest)],
        nextRound: List[(Id[_], Rest)]
    ): Map[Id[_], Int] =
      todo match {
        case Nil =>
          nextRound match {
            case Nil => state
            case repeat =>
              val sortRepeat = repeat.sortWith { case ((i0, r0), (i1, r1)) =>
                r1.dependsOn(i0) || (!r0.dependsOn(i1))
              }
              lookup(state, sortRepeat, Nil)
          }
        case (h @ (id, Same(a))) :: rest =>
          state.get(a) match {
            case Some(depth) =>
              val state1 = state.updated(id, depth)
              lookup(state1, rest, nextRound)
            case None =>
              lookup(state, rest, h :: nextRound)
          }
        case (h @ (id, Inc(a))) :: rest =>
          state.get(a) match {
            case Some(depth) =>
              val state1 = state.updated(id, depth + 1)
              lookup(state1, rest, nextRound)
            case None =>
              lookup(state, rest, h :: nextRound)
          }
        case (h @ (id, MaxInc(a, b))) :: rest =>
          (state.get(a), state.get(b)) match {
            case (Some(da), Some(db)) =>
              val depth = math.max(da, db) + 1
              val state1 = state.updated(id, depth)
              lookup(state1, rest, nextRound)
            case _ =>
              lookup(state, rest, h :: nextRound)
          }
        case (id, Variadic(Nil)) :: rest =>
          val depth = 0
          val state1 = state.updated(id, depth)
          lookup(state1, rest, nextRound)
        case (item @ (id, Variadic(h :: t))) :: rest =>
          // max can't throw here because ids is non-empty
          def maxId(head: Id[_], tail: List[Id[_]], acc: Int): Option[Int] =
            state.get(head) match {
              case None => None
              case Some(d) =>
                val newAcc = Math.max(acc, d)
                tail match {
                  case Nil    => Some(newAcc)
                  case h :: t => maxId(h, t, newAcc)
                }
            }

          maxId(h, t, 0) match {
            case Some(depth) =>
              val state1 = state.updated(id, depth + 1)
              lookup(state1, rest, nextRound)
            case None =>
              lookup(state, rest, item :: nextRound)
          }
      }

    @annotation.tailrec
    def loop(
        stack: List[Id[_]],
        seen: Set[Id[_]],
        state: Map[Id[_], Int],
        todo: List[(Id[_], Rest)]
    ): Map[Id[_], Int] =
      stack match {
        case Nil =>
          lookup(state, todo, Nil)
        case h :: tail if seen(h) => loop(tail, seen, state, todo)
        case h :: tail =>
          val seen1 = seen + h
          idToExp.get(h) match {
            case None =>
              loop(tail, seen1, state, todo)
            case Some(Expr.Const(_)) =>
              loop(tail, seen1, state.updated(h, 0), todo)
            case Some(Expr.Var(id)) =>
              loop(id :: tail, seen1, state, (h, Same(id)) :: todo)
            case Some(Expr.Unary(id, _)) =>
              loop(id :: tail, seen1, state, (h, Inc(id)) :: todo)
            case Some(Expr.Binary(id0, id1, _)) =>
              loop(id0 :: id1 :: tail, seen1, state, (h, MaxInc(id0, id1)) :: todo)
            case Some(Expr.Variadic(ids, _)) =>
              loop(ids reverse_::: tail, seen1, state, (h, Variadic(ids)) :: todo)
          }
      }

    loop(roots.toList, Set.empty, Map.empty, Nil)
  }

  /**
   * Find all the nodes currently in the graph
   */
  lazy val allNodes: Set[N[_]] = {
    type Node = Either[Id[_], Expr[N, _]]
    def deps(n: Node): List[Node] = n match {
      case Right(Expr.Const(_))            => Nil
      case Right(Expr.Var(id))             => Left(id) :: Nil
      case Right(Expr.Unary(id, _))        => Left(id) :: Nil
      case Right(Expr.Binary(id0, id1, _)) => Left(id0) :: Left(id1) :: Nil
      case Right(Expr.Variadic(ids, _))    => ids.map(Left(_))
      case Left(id)                        => idToExp.get(id).map(Right(_): Node).toList
    }
    val all = Graphs.reflexiveTransitiveClosure(roots.toList.map(Left(_): Node))(deps _)

    all.iterator.collect { case Right(expr) => evalMemo(expr) }.toSet
  }

  ////////////////////////////
  //
  //  These following methods are the only methods that directly
  //  allocate new Dag instances. These are where all invariants
  //  must be maintained
  //
  ////////////////////////////

  /**
   * Add a GC root, or tail in the DAG, that can never be deleted.
   */
  def addRoot[T](node: N[T]): (Dag[N], Id[T]) = {
    val (dag, id) = ensure(node)
    (dag.copy(gcroots = dag.roots + id), id)
  }

  // Convenient method to produce new, modified DAGs based on this
  // one.
  private def copy(
      id2Exp: HMap[Id, Expr[N, *]] = self.idToExp,
      node2Literal: FunctionK[N, Literal[N, *]] = self.toLiteral,
      gcroots: Set[Id[_]] = self.roots
  ): Dag[N] = new Dag[N] {
    def idToExp = id2Exp
    def roots = gcroots
    def toLiteral = node2Literal
  }

  // these are included for binary compatibility

  // $COVERAGE-OFF$
  private[dagon] def com$stripe$dagon$Dag$$copy$default$2()
      : com.twitter.scalding.dagon.FunctionK[N, Literal[N, *]] =
    self.toLiteral

  private[dagon] def com$stripe$dagon$Dag$$copy$default$3(): scala.collection.immutable.Set[Id[_]] =
    self.roots
  // $COVERAGE-ON$

  // Produce a new DAG that is equivalent to this one, but which frees
  // orphaned nodes and other internal state which may no longer be
  // needed.
  private def gc: Dag[N] = {
    val keepers = reachableIds
    if (idToExp.forallKeys(keepers)) this
    else copy(id2Exp = idToExp.filterKeys(keepers))
  }

  /*
   * This updates the canonical Id for a given node and expression
   */
  protected def replaceId[A](newId: Id[A], expr: Expr[N, A], node: N[A]): Dag[N] =
    copy(id2Exp = idToExp.updated(newId, expr))

  protected def repointIds[A](orig: N[A], oldIds: Iterable[Id[A]], newId: Id[A], newNode: N[A]): Dag[N] =
    if (oldIds.nonEmpty) {
      val newIdToExp = oldIds.foldLeft(idToExp) { (mapping, origId) =>
        mapping.updated(origId, Expr.Var[N, A](newId))
      }
      copy(id2Exp = newIdToExp).gc
    } else this

  /**
   * This is only called by ensure
   *
   * Note, Expr must never be a Var
   */
  private def addExp[T](exp: Expr[N, T]): (Dag[N], Id[T]) = {
    require(!exp.isVar)
    val nodeId = Id.next[T]()
    (copy(id2Exp = idToExp.updated(nodeId, exp)), nodeId)
  }

  ////////////////////////////
  //
  // End of methods that direcly allocate new Dag instances
  //
  ////////////////////////////

  /**
   * This finds an Id[T] in the current graph that is equivalent to the given N[T]
   */
  def find[T](node: N[T]): Option[Id[T]] =
    findLiteral(toLiteral(node), node)

  private def findLiteral[T](lit: Literal[N, T], n: => N[T]): Option[Id[T]] =
    litToId.getOrElseUpdate(
      lit, {
        // It's important to not compare equality in the Literal
        // space because it can have function members that are
        // equivalent, but not .equals
        val lst = findAll(n).filterNot(id => idToExp(id).isVar)
        val it = lazyListToIterator(lst)
        if (it.hasNext) {
          // there can be duplicate ids. Consider this case:
          // Id(0) -> Expr.Unary(Id(1), fn)
          // Id(1) -> Expr.Const(n1)
          // Id(2) -> Expr.Unary(Id(3), fn)
          // Id(3) -> Expr.Const(n2)
          //
          // then, a rule replaces n1 and n2 both with n3 Then, we'd have
          // Id(1) -> Var(Id(4))
          // Id(4) -> Expr.Const(n3)
          // Id(3) -> Var(Id(4))
          //
          // and now, Id(0) and Id(2) both point to non-Var nodes, but also
          // both are equal

          // We use the maximum ID which is important to deal with
          // cycle avoidance in applyRule since we guarantee
          // that all the nodes that are repointed are computed
          // before we add a new node to graph
          Some(it.max)
        } else {
          // if the node is the in the graph it has at least
          // one non-Var node
          None
        }
      }
    )

  /**
   * Nodes can have multiple ids in the graph, this gives all of them
   */
  def findAll[T](node: N[T]): LazyList[Id[T]] = {
    // TODO: this computation is really expensive, 60% of CPU in a recent benchmark
    // maintaining these mappings would be nice, but maybe expensive as we are rewriting
    // nodes
    val f = new FunctionK[HMap[Id, Expr[N, *]]#Pair, Lambda[x => Option[Id[x]]]] {
      def toFunction[T1] = { case (thisId, expr) =>
        if (node == evalMemo(expr)) Some(thisId) else None
      }
    }

    // this cast is safe if node == expr.evaluate(idToExp) implies types match
    idToExp.optionMap(f).asInstanceOf[LazyList[Id[T]]]
  }

  /**
   * This throws if the node is missing, use find if this is not a logic error in your programming. With
   * dependent types we could possibly get this to not compile if it could throw.
   */
  def idOf[T](node: N[T]): Id[T] =
    find(node).getOrElse {
      val msg = s"could not get node: $node\n from $this"
      throw new NoSuchElementException(msg)
    }

  /**
   * ensure the given literal node is present in the Dag Note: it is important that at each moment, each node
   * has at most one id in the graph. Put another way, for all Id[T] in the graph evaluate(id) is distinct.
   */
  protected def ensure[T](node: N[T]): (Dag[N], Id[T]) = {
    val lit = toLiteral(node)
    val litMemo = Literal.evaluateMemo[N]
    try ensureFast(lit, litMemo)
    catch {
      case _: Throwable => // StackOverflowError should work, but not on scala.js
        ensureRec(lit, litMemo).result
    }
  }

  /*
   * This does recursion on the stack, which is faster, but can overflow
   */
  protected def ensureFast[T](lit: Literal[N, T], memo: FunctionK[Literal[N, *], N]): (Dag[N], Id[T]) =
    findLiteral(lit, memo(lit)) match {
      case Some(id) => (this, id)
      case None =>
        lit match {
          case Literal.Const(n) =>
            addExp(Expr.Const(n))
          case Literal.Unary(prev, fn) =>
            val (exp1, idprev) = ensureFast(prev, memo)
            exp1.addExp(Expr.Unary(idprev, fn))
          case Literal.Binary(n1, n2, fn) =>
            val (exp1, id1) = ensureFast(n1, memo)
            val (exp2, id2) = exp1.ensureFast(n2, memo)
            exp2.addExp(Expr.Binary(id1, id2, fn))
          case Literal.Variadic(args, fn) =>
            @annotation.tailrec
            def go[A](dag: Dag[N], args: List[Literal[N, A]], acc: List[Id[A]]): (Dag[N], List[Id[A]]) =
              args match {
                case Nil => (dag, acc.reverse)
                case h :: tail =>
                  val (dag1, hid) = dag.ensureFast(h, memo)
                  go(dag1, tail, hid :: acc)
              }

            val (d, ids) = go(this, args, Nil)
            d.addExp(Expr.Variadic(ids, fn))
        }
    }

  protected def ensureRec[T](
      lit: Literal[N, T],
      memo: FunctionK[Literal[N, *], N]
  ): TailCalls.TailRec[(Dag[N], Id[T])] =
    findLiteral(lit, memo(lit)) match {
      case Some(id) => TailCalls.done((this, id))
      case None =>
        lit match {
          case Literal.Const(n) =>
            TailCalls.done(addExp(Expr.Const(n)))
          case Literal.Unary(prev, fn) =>
            TailCalls.tailcall(ensureRec(prev, memo)).map { case (exp1, idprev) =>
              exp1.addExp(Expr.Unary(idprev, fn))
            }
          case Literal.Binary(n1, n2, fn) =>
            for {
              p1 <- TailCalls.tailcall(ensureRec(n1, memo))
              (exp1, id1) = p1
              p2 <- TailCalls.tailcall(exp1.ensureRec(n2, memo))
              (exp2, id2) = p2
            } yield exp2.addExp(Expr.Binary(id1, id2, fn))
          case Literal.Variadic(args, fn) =>
            def go[A](dag: Dag[N], args: List[Literal[N, A]]): TailCalls.TailRec[(Dag[N], List[Id[A]])] =
              args match {
                case Nil => TailCalls.done((dag, Nil))
                case h :: tail =>
                  for {
                    rest <- go(dag, tail)
                    (dag1, its) = rest
                    dagH <- TailCalls.tailcall(dag1.ensureRec(h, memo))
                    (dag2, idh) = dagH
                  } yield (dag2, idh :: its)
              }

            go(this, args).map { case (d, ids) =>
              d.addExp(Expr.Variadic(ids, fn))
            }
        }
    }

  /**
   * After applying rules to your Dag, use this method to get the original node type. Only call this on an
   * Id[T] that was generated by this dag or a parent.
   */
  def evaluate[T](id: Id[T]): N[T] =
    evaluateOption(id).getOrElse {
      val msg = s"Could not evaluate: $id\nin $this"
      throw new NoSuchElementException(msg)
    }

  def evaluateOption[T](id: Id[T]): Option[N[T]] =
    idToN.getOrElseUpdate(id, idToExp.get(id).map(evalMemo(_)))

  /**
   * Return the number of nodes that depend on the given Id, TODO we might want to cache these. We need to
   * garbage collect nodes that are no longer reachable from the root
   */
  def fanOut(id: Id[_]): Int =
    evaluateOption(id)
      .map(fanOut)
      .getOrElse(0)

  /**
   * Returns 0 if the node is absent, which is true use .contains(n) to check for containment
   */
  def fanOut(node: N[_]): Int = {
    val interiorFanOut = dependentsOf(node).size
    val tailFanOut = if (isRoot(node)) 1 else 0
    interiorFanOut + tailFanOut
  }

  /**
   * Is this node a root of this graph
   */
  def isRoot(n: N[_]): Boolean =
    roots.iterator.exists(evaluatesTo(_, n))

  // This is a roots up iterator giving the depth
  // to the nearest root and in sorted by Id.serial
  // within each depth
  private def rootsUp: Iterator[(Int, Id[_])] = {
    type State = (Int, Id[_], List[Id[_]], List[Id[_]], Set[Id[_]])

    def sort(l: List[Id[_]]): List[Id[_]] =
      l.asInstanceOf[List[Id[Any]]].sorted

    def computeNext(s: List[Id[_]], seen: Set[Id[_]]): (List[Id[_]], Set[Id[_]]) =
      s.foldLeft((List.empty[Id[_]], seen)) { case ((l, s), id) =>
        val newIds = Expr.dependsOnIds(idToExp(id)).filterNot(seen)
        (newIds reverse_::: l, s ++ newIds)
      }

    def initState: Option[State] = {
      val rootList = roots.toList
      val (next, seen) = computeNext(rootList, rootList.toSet)
      sort(rootList) match {
        case Nil       => None
        case h :: tail => Some((0, h, tail, next, seen))
      }
    }

    def nextState(current: State): Option[State] =
      current match {
        case (_, _, Nil, Nil, _) =>
          None
        case (depth, _, Nil, nextBatch, seen) =>
          sort(nextBatch) match {
            case h :: tail =>
              val (nextBatch1, seen1) = computeNext(nextBatch, seen)
              Some((depth + 1, h, tail, nextBatch1, seen1))
            case Nil =>
              // nextBatch has at least one item, and sorting preserves that
              sys.error("impossible")
          }
        case (d, _, h :: tail, next, seen) =>
          Some((d, h, tail, next, seen))
      }

    new Iterator[(Int, Id[_])] {
      var state: Option[State] = initState

      def hasNext = state.isDefined
      def next: (Int, Id[_]) =
        state match {
          case None => throw new NoSuchElementException("roots up has no more items")
          case Some(s) =>
            state = nextState(s)
            (s._1, s._2)
        }
    }
  }

  /**
   * Is this node in this DAG
   */
  def contains(node: N[_]): Boolean =
    find(node).isDefined

  /**
   * What nodes do we depend directly on
   */
  def dependenciesOf(node: N[_]): List[N[_]] =
    toLiteral(node) match {
      case Literal.Const(_) =>
        Nil
      case Literal.Unary(n, _) =>
        n.evaluate :: Nil
      case Literal.Binary(n1, n2, _) =>
        val evalLit = Literal.evaluateMemo[N]
        evalLit(n1) :: evalLit(n2) :: Nil
      case Literal.Variadic(inputs, _) =>
        val evalLit = Literal.evaluateMemo[N]
        inputs.map(evalLit(_))
    }

  /**
   * It is as expensive to compute this for the whole graph as it is to answer a single query we already cache
   * the N pointed to, so this structure should be small
   */
  private lazy val dependencyMap: Map[N[_], Set[N[_]]] = {
    def dependsOnSet(expr: Expr[N, _]): Set[N[_]] = expr match {
      case Expr.Const(_)            => Set.empty
      case Expr.Var(id)             => sys.error(s"logic error: Var($id)")
      case Expr.Unary(id, _)        => Set(evaluate(id))
      case Expr.Binary(id0, id1, _) => Set(evaluate(id0), evaluate(id1))
      case Expr.Variadic(ids, _)    => ids.iterator.map(evaluate(_)).toSet
    }

    type SetConst[T] = (N[T], Set[N[_]])
    val pointsToNode = new FunctionK[HMap[Id, Expr[N, *]]#Pair, Lambda[x => Option[SetConst[x]]]] {
      def toFunction[T] = { case (id, expr) =>
        // here are the nodes we depend on:

        // We can ignore Vars here, since all vars point to a final expression
        if (!expr.isVar) {
          val depSet = dependsOnSet(expr)
          Some((evalMemo(expr), depSet))
        } else None
      }
    }

    idToExp
      .optionMap[SetConst](pointsToNode)
      .flatMap { case (n, deps) =>
        deps.map((_, n): (N[_], N[_]))
      }
      .groupBy(_._1)
      .iterator
      .map { case (k, vs) => (k, vs.iterator.map(_._2).toSet) }
      .toMap
  }

  /**
   * list all the nodes that depend on the given node
   */
  def dependentsOf(node: N[_]): Set[N[_]] =
    dependencyMap.getOrElse(node, Set.empty)

  private def evaluatesTo[A, B](id: Id[A], n: N[B]): Boolean = {
    val idN = evaluate(id)
    // since we cache, reference equality will often work
    val refEq = idN.asInstanceOf[AnyRef] eq id.asInstanceOf[AnyRef]
    refEq || (idN == n)
  }

  /**
   * equivalent to (but maybe faster than) fanOut(n) <= 1
   */
  def hasSingleDependent(n: N[_]): Boolean =
    fanOut(n) <= 1

  /**
   * Return all dependents of a given node. Does not include itself
   */
  def transitiveDependentsOf(p: N[_]): Set[N[_]] = {
    def nfn(n: N[Any]): List[N[Any]] =
      dependentsOf(n).toList.asInstanceOf[List[N[Any]]]

    Graphs.depthFirstOf(p.asInstanceOf[N[Any]])(nfn _).toSet
  }

  /**
   * Return the transitive dependencies of a given node
   */
  def transitiveDependenciesOf(p: N[_]): Set[N[_]] = {
    def nfn(n: N[Any]): List[N[Any]] =
      dependenciesOf(n).toList.asInstanceOf[List[N[Any]]]

    Graphs.depthFirstOf(p.asInstanceOf[N[Any]])(nfn _).toSet
  }
}

object Dag {
  def empty[N[_]](n2l: FunctionK[N, Literal[N, *]]): Dag[N] =
    new Dag[N] {
      val idToExp = HMap.empty[Id, Expr[N, *]]
      val toLiteral = n2l
      val roots = Set.empty[Id[_]]
    }

  /**
   * This creates a new Dag rooted at the given tail node
   */
  def apply[T, N[_]](n: N[T], nodeToLit: FunctionK[N, Literal[N, *]]): (Dag[N], Id[T]) =
    empty(nodeToLit).addRoot(n)

  /**
   * This is the most useful function. Given a N[T] and a way to convert to Literal[T, N], apply the given
   * rule until it no longer applies, and return the N[T] which is equivalent under the given rule
   */
  def applyRule[T, N[_]](n: N[T], nodeToLit: FunctionK[N, Literal[N, *]], rule: Rule[N]): N[T] = {
    val (dag, id) = apply(n, nodeToLit)
    dag(rule).evaluate(id)
  }

  /**
   * This is useful when you have rules you want applied in a certain order. Given a N[T] and a way to convert
   * to Literal[T, N], for each rule in the sequence, apply the given rule until it no longer applies, and
   * return the N[T] which is equivalent under the given rule
   */
  def applyRuleSeq[T, N[_]](n: N[T], nodeToLit: FunctionK[N, Literal[N, *]], rules: Seq[Rule[N]]): N[T] = {
    val (dag, id) = apply(n, nodeToLit)
    dag.applySeq(rules).evaluate(id)
  }
}
