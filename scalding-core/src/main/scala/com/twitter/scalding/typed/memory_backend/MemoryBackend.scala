package com.twitter.scalding.typed.memory_backend

import cascading.flow.{ FlowDef, FlowConnector }
import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.{ Fields, TupleEntryIterator }
import com.twitter.scalding.typed._
import com.twitter.scalding.{ Config, Execution, ExecutionCounters, Mode }
import java.util.concurrent.atomic.AtomicReference
import java.util.{ ArrayList, Collections, UUID }
import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext, Promise }
import scala.collection.mutable.{ ArrayBuffer, Map => MMap }
import scala.collection.JavaConverters._

import Execution.{ ToWrite, Writer }

class AtomicBox[T <: AnyRef](init: T) {
  private[this] val ref = new AtomicReference[T](init)

  def lazySet(t: T): Unit =
    ref.lazySet(t)

  /**
   * use a pure function to update the state.
   * fn may be called more than once
   */
  def update[R](fn: T => (T, R)): R = {

    @annotation.tailrec
    def loop(): R = {
      val init = ref.get
      val (next, res) = fn(init)
      if (ref.compareAndSet(init, next)) res
      else loop()
    }

    loop()
  }

  def get(): T = ref.get
}

final class MemoryMode private (srcs: Map[TypedSource[_], Iterable[_]], sinks: Map[TypedSink[_], AtomicBox[Option[Iterable[_]]]]) extends Mode {

  def newWriter(): Writer =
    new MemoryWriter(this)

  /**
   * note that ??? in scala is the same as not implemented
   *
   * These methods are not needed for use with the Execution API, and indeed
   * don't make sense outside of cascading, but backwards compatibility
   * currently requires them on Mode. Ideally we will find another solution
   * to this in the future
   */
  def openForRead(config: Config, tap: Tap[_, _, _]): TupleEntryIterator = ???
  def fileExists(filename: String): Boolean = ???
  def newFlowConnector(props: Config): FlowConnector = ???

  def addSource[T](src: TypedSource[T], ts: Iterable[T]): MemoryMode =
    new MemoryMode(srcs + (src -> ts), sinks)

  def addSink[T](sink: TypedSink[T]): MemoryMode =
    new MemoryMode(srcs, sinks + (sink -> new AtomicBox[Option[Iterable[_]]](None)))

  /**
   * This has a side effect of mutating this MemoryMode
   */
  def writeSink[T](t: TypedSink[T], iter: Iterable[T]): Unit =
    sinks(t).lazySet(Some(iter))

  def readSink[T](t: TypedSink[T]): Option[Iterable[T]] =
    sinks.get(t).flatMap(_.get).asInstanceOf[Option[Iterable[T]]]

  def readSource[T](t: TypedSource[T]): Option[Iterable[T]] =
    srcs.get(t).asInstanceOf[Option[Iterable[T]]]
}

object MemoryMode {
  def empty: MemoryMode = new MemoryMode(Map.empty, Map.empty)
}

object MemoryPlanner {

  sealed trait Op[+O] {
    def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[_ <: O]]

    def concatMap[O1](fn: O => TraversableOnce[O1]): Op[O1] =
      transform { in: IndexedSeq[O] =>
        val res = ArrayBuffer[O1]()
        val it = in.iterator
        while(it.hasNext) {
          val i = it.next
          fn(i).foreach(res += _)
        }
        res
      }

    def map[O1](fn: O => O1): Op[O1] =
      transform { in: IndexedSeq[O] =>
        val res = new ArrayBuffer[O1](in.size)
        val it = in.iterator
        while(it.hasNext) {
          res += fn(it.next)
        }
        res
      }

    def filter(fn: O => Boolean): Op[O] =
      transform { in: IndexedSeq[O] =>
        // we can't increase in size, just assume the worst
        val res = new ArrayBuffer[O](in.size)
        val it = in.iterator
        while(it.hasNext) {
          val o = it.next
          if (fn(o)) res += o
        }
        res
      }

    def transform[O1 >: O, O2](fn: IndexedSeq[O1] => ArrayBuffer[O2]): Op[O2] =
      Op.Transform[O1, O2](this, fn)
  }
  object Op {
    def source[I](i: Iterable[I]): Op[I] = Source(_ => Future.successful(i))
    def empty[I]: Op[I] = source(Nil)

    final case class Source[I](input: ConcurrentExecutionContext => Future[Iterable[I]]) extends Op[I] {
      private[this] val promise: Promise[ArrayBuffer[I]] = Promise()

      def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[I]] = {
        if (!promise.isCompleted) {
          promise.tryCompleteWith {
            val iter = input(cec)
            iter.map { i => ArrayBuffer.concat(i) }
          }
        }

        promise.future
      }
    }

    final case class Materialize[O](op: Op[O]) extends Op[O] {
      private[this] val promise: Promise[ArrayBuffer[_ <: O]] = Promise()

      def result(implicit cec: ConcurrentExecutionContext) = {
        if (!promise.isCompleted) {
          promise.tryCompleteWith(op.result)
        }

        promise.future
      }
    }

    final case class Concat[O](left: Op[O], right: Op[O]) extends Op[O] {
      def result(implicit cec: ConcurrentExecutionContext) = {
        val f1 = left.result
        val f2 = right.result
        f1.zip(f2).map { case (l, r) => ArrayBuffer.concat(l, r) }
      }
    }

    final case class Map[I, O](input: Op[I], fn: I => TraversableOnce[O]) extends Op[O] {
      def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[O]] =
        input.result.map { array =>
          val res = ArrayBuffer[O]()
          val it = array.iterator
          while(it.hasNext) {
            val i = it.next
            fn(i).foreach(res += _)
          }
          res
        }
    }

    final case class OnComplete[O](of: Op[O], fn: () => Unit) extends Op[O] {
      def result(implicit cec: ConcurrentExecutionContext) = {
        val res = of.result
        res.onComplete(_ => fn())
        res
      }
    }

    final case class Transform[I, O](input: Op[I], fn: IndexedSeq[I] => ArrayBuffer[O]) extends Op[O] {
      def result(implicit cec: ConcurrentExecutionContext) =
        input.result.map(fn)
    }

    final case class Reduce[K, V1, V2](
      input: Op[(K, V1)],
      fn: (K, Iterator[V1]) => Iterator[V2],
      ord: Option[Ordering[_ >: V1]]
      ) extends Op[(K, V2)] {

      def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[(K, V2)]] =
        input.result.map { kvs =>
          val valuesByKey = MMap[K, ArrayList[V1]]()
          def add(kv: (K, V1)): Unit = {
            val vs = valuesByKey.getOrElseUpdate(kv._1, new ArrayList[V1]())
            vs.add(kv._2)
          }
          kvs.foreach(add)

          /*
           * This portion could be parallelized for each key, or we could split
           * the keys into as many groups as there are CPUs and process that way
           */
          val res = ArrayBuffer[(K, V2)]()
          valuesByKey.foreach { case (k, vs) =>
            ord.foreach(Collections.sort[V1](vs, _))
            val v2iter = fn(k, vs.iterator.asScala)
            while(v2iter.hasNext) {
              res += ((k, v2iter.next))
            }
          }
          res
      }
    }

    final case class Join[A, B, C](
      opA: Op[A],
      opB: Op[B],
      fn: (IndexedSeq[A], IndexedSeq[B]) => ArrayBuffer[C]) extends Op[C] {

      def result(implicit cec: ConcurrentExecutionContext) = {
        // start both futures in parallel
        val f1 = opA.result
        val f2 = opB.result
        f1.zip(f2).map { case (a, b) => fn(a, b) }
      }
    }
  }

  /**
   * Memoize previously planned TypedPipes so we don't replan too much
   *
   * ideally we would look at a series of writes and first look for all
   * the fan-outs and only memoize at the spots where we have to force
   * materializations, but this is just a demo for now
   */
  case class Memo(planned: Map[TypedPipe[_], Op[_]]) {
    def plan[T](t: TypedPipe[T])(op: => (Memo, Op[T])): (Memo, Op[T]) =
      planned.get(t) match {
        case Some(op) => (this, op.asInstanceOf[Op[T]])
        case None =>
          val (m1, newOp) = op
          (m1.copy(planned = m1.planned.updated(t, newOp)), newOp)
      }
  }

  object Memo {
    def empty: Memo = Memo(Map.empty)
  }

}

/**
 * These are just used as type markers which are connected
 * to inputs via the MemoryMode
 */
case class SourceT[T](ident: String) extends TypedSource[T] {
  /**
   * note that ??? in scala is the same as not implemented
   *
   * These methods are not needed for use with the Execution API, and indeed
   * don't make sense outside of cascading, but backwards compatibility
   * currently requires them on TypedSource. Ideally we will find another solution
   * to this in the future
   */
  def converter[U >: T] = ???
  def read(implicit flowDef: FlowDef, mode: Mode): Pipe = ???
}

/**
 * These are just used as type markers which are connected
 * to outputs via the MemoryMode
 */
case class SinkT[T](indent: String) extends TypedSink[T] {
  /**
   * note that ??? in scala is the same as not implemented
   *
   * These methods are not needed for use with the Execution API, and indeed
   * don't make sense outside of cascading, but backwards compatibility
   * currently requires them on TypedSink. Ideally we will find another solution
   * to this in the future
   */
  def setter[U <: T] = ???
  def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): Pipe = ???
}

class MemoryWriter(mem: MemoryMode) extends Writer {

  import MemoryPlanner.{ Memo, Op }
  import TypedPipe._

  def start(): Unit = ()
  /**
   * This is called by an Execution to end processing
   */
  def finished(): Unit = ()

  def plan[T](m: Memo, tp: TypedPipe[T]): (Memo, Op[T]) =
    m.plan(tp) {
      tp match {
        case CounterPipe(pipe) =>
          // TODO: counters not yet supported, but can be with an concurrent hashmap
          plan(m, pipe.map(_._1))
        case cp@CrossPipe(_, _) =>
          plan(m, cp.viaHashJoin)

        case CrossValue(left, EmptyValue) => (m, Op.empty)
        case CrossValue(left, LiteralValue(v)) =>
          val (m1, op) = plan(m, left) // linter:disable:UndesirableTypeInference
          (m1, op.concatMap { a => Iterator.single((a, v)) })
        case CrossValue(left, ComputedValue(right)) =>
          plan(m, CrossPipe(left, right))
        case DebugPipe(p) =>
          // There is really little that can be done here but println
          plan(m, p.map { t => println(t); t })

        case EmptyTypedPipe =>
          // just use an empty iterable pipe.
          // Note, rest is irrelevant
          (m, Op.empty[T])

        case fk@FilterKeys(_, _) =>
          def go[K, V](node: FilterKeys[K, V]): (Memo, Op[(K, V)]) = {
            val FilterKeys(pipe, fn) = node
            val (m1, op) = plan(m, pipe)
            (m1, op.concatMap { case (k, v) => if (fn(k)) { (k, v) :: Nil } else Nil })
          }
          go(fk)

        case f@Filter(_, _) =>
          def go[T](f: Filter[T]): (Memo, Op[T]) = {
            val Filter(p, fn) = f
            val (m1, op) = plan(m, f)
            (m1, op.filter(fn))
          }
          go(f)

        case f@FlatMapValues(_, _) =>
          def go[K, V, U](node: FlatMapValues[K, V, U]) = {
            val fn = node.fn
            val (m1, op) = plan(m, node.input)
            (m1, op.concatMap { case (k, v) => fn(v).map((k, _)) })
          }

          go(f)

        case FlatMapped(prev, fn) =>
          val (m1, op) = plan(m, prev) // linter:disable:UndesirableTypeInference
          (m1, op.concatMap(fn))

        case ForceToDisk(pipe) =>
          val (m1, op) = plan(m, pipe)
          (m1, Op.Materialize(op))

        case Fork(pipe) =>
          val (m1, op) = plan(m, pipe)
          (m1, Op.Materialize(op))

        case IterablePipe(iterable) =>
          (m, Op.source(iterable))

        case f@MapValues(_, _) =>
          def go[K, V, U](node: MapValues[K, V, U]) = {
            val mvfn = node.fn
            val (m1, op) = plan(m, node.input)
            (m1, op.map { case (k, v) => (k, mvfn(v)) })
          }

          go(f)

        case Mapped(input, fn) =>
          val (m1, op) = plan(m, input) // linter:disable:UndesirableTypeInference
          (m1, op.map(fn))

        case MergedTypedPipe(left, right) =>
          val (m1, op1) = plan(m, left)
          val (m2, op2) = plan(m1, right)
          (m2, Op.Concat(op1, op2))

        case SourcePipe(src) =>
          (m, Op.Source({ cec =>
            mem.readSource(src) match {
              case Some(iter) => Future.successful(iter)
              case None => Future.failed(new Exception(s"Source: $src not wired. Please provide an input with MemoryMode.addSource"))
            }
          }))

        case slk@SumByLocalKeys(_, _) =>
          def sum[K, V](sblk: SumByLocalKeys[K, V]) = {
            val SumByLocalKeys(p, sg) = sblk

            val (m1, op) = plan(m, p)
            (m1, op.transform[(K, V), (K, V)] { kvs =>
              val map = collection.mutable.Map.empty[K, V]
              val iter = kvs.iterator
              while(iter.hasNext) {
                val (k, v) = iter.next
                map(k) = map.get(k) match {
                  case None => v
                  case Some(v1) => sg.plus(v1, v)
                }
              }
              val res = new ArrayBuffer[(K, V)](map.size)
              map.foreach { res += _ }
              res
            })
          }
          sum(slk)

        case tp@TrappedPipe(_, _, _) => ???
          // this can be interpretted as catching any exception
          // on the map-phase until the next partition, so it can
          // be made to work, but skipping for now

        case WithDescriptionTypedPipe(pipe, description, dedup) =>
          plan(m, pipe)

        case WithOnComplete(pipe, fn) =>
          val (m1, op) = plan(m, pipe)
          (m1, Op.OnComplete(op, fn))

        case hcg@HashCoGroup(_, _, _) =>
          def go[K, V1, V2, R](hcg: HashCoGroup[K, V1, V2, R]) = {
            val (m1, leftOp) = plan(m, hcg.left)
            val (m2, rightOp) = planHashJoinable(m1, hcg.right)
            (m2, Op.Join[(K, V1), (K, V2), (K, R)](leftOp, rightOp, { (v1s, v2s) =>
              val kv2 = v2s.groupBy(_._1)
              val result = new ArrayBuffer[(K, R)]()
              v1s.foreach { case (k, v1) =>
                val v2 = kv2.getOrElse(k, Nil).map(_._2)
                result ++= hcg.joiner(k, v1, v2).map((k, _))
              }
              result
            }))
          }
          go(hcg)

        case CoGroupedPipe(cg) =>
          def go[K, V](cg: CoGrouped[K, V]) = {
            val inputs = cg.inputs
            val joinf = cg.joinFunction
            /**
             * we need to expand the Op type to deal
             * with multi-way or we need to keep
             * the decomposed series of joins
             *
             * TODO
             * implement cogroups on the memory platform
             */
            ???
          }
          go(cg)

        case ReduceStepPipe(IdentityReduce(_, pipe, _, descriptions)) =>
          plan(m, pipe)
        case ReduceStepPipe(UnsortedIdentityReduce(_, pipe, _, descriptions)) =>
          plan(m, pipe)
        case ReduceStepPipe(IdentityValueSortedReduce(_, pipe, ord, _, _)) =>
          def go[K, V](p: TypedPipe[(K, V)], ord: Ordering[_ >: V]) = {
            val (m1, op) = plan(m, p)
            (m1, Op.Reduce[K, V, V](op, { (k, vs) => vs }, Some(ord)))
          }
          go(pipe, ord)
        case ReduceStepPipe(ValueSortedReduce(_, pipe, ord, fn, _, _)) =>
          val (m1, op) = plan(m, pipe)
          (m1, Op.Reduce(op, fn, Some(ord)))
        case ReduceStepPipe(IteratorMappedReduce(_, pipe, fn, _, _)) =>
          val (m1, op) = plan(m, pipe)
          (m1, Op.Reduce(op, fn, None))
      }
    }

  def planHashJoinable[K, V](m: Memo, hk: HashJoinable[K, V]): (Memo, Op[(K, V)]) = hk match {
    case IdentityReduce(_, pipe, _, _) => plan(m, pipe)
    case UnsortedIdentityReduce(_, pipe, _, _) => plan(m, pipe)
    case imr@IteratorMappedReduce(_, _, _, _, _) =>
      def go[K, U, V](imr: IteratorMappedReduce[K, U, V]) = {
        val IteratorMappedReduce(_, pipe, fn, _, _) = imr
        val (m1, op) = plan(m, pipe)
        (m1, op.transform[(K, U), (K, V)] { kvs =>
          val m = kvs.groupBy(_._1).iterator
          val res = ArrayBuffer[(K, V)]()
          m.foreach { case (k, kus) =>
            val us = kus.iterator.map { case (k, u) => u }
            fn(k, us).foreach { v =>
              res += ((k, v))
            }
          }
          res
        })
      }
      go(imr)
  }

  private[this] case class State(
    id: Long,
    memo: MemoryPlanner.Memo,
    forced: Map[TypedPipe[_], Future[TypedPipe[_]]]
    )

  private[this] val state = new AtomicBox[State](State(0, MemoryPlanner.Memo.empty, Map.empty))

  /**
   * do a batch of writes, possibly optimizing, and return a new unique
   * Long.
   *
   * empty writes are legitmate and should still return a Long
   */
  def execute(
    conf: Config,
    mode: Mode,
    writes: List[ToWrite])(implicit cec: ConcurrentExecutionContext): Future[(Long, ExecutionCounters)] = {

      type Action = () => Future[Unit]

      def force[T](p: TypedPipe[T], oldState: State): (State, Action) = {
        val (nextM, op) = plan(oldState.memo, p)
        val pipePromise = Promise[TypedPipe[Any]]()
        val action = () => {
          val arrayBufferF = op.result
          pipePromise.completeWith(arrayBufferF.map(TypedPipe.from(_)))

          arrayBufferF.map(_ => ())
        }
        (oldState.copy(memo = nextM, forced = oldState.forced.updated(p, pipePromise.future)), action)
      }

      val (id, acts) = state.update { s =>
        val (nextState, acts) = writes.foldLeft((s, List.empty[Action])) {
          case (noop, ToWrite.Force(pipe)) if noop._1.forced.contains(pipe) =>
            // we have already forced this pipe
            noop
          case ((oldState, acts), ToWrite.Force(pipe)) =>
            val (st, a) = force(pipe, oldState)
            (st, a :: acts)
          case (old@(oldState, acts), ToWrite.ToIterable(pipe)) =>
            pipe match {
              case TypedPipe.EmptyTypedPipe => old
              case TypedPipe.IterablePipe(_) => old
              case TypedPipe.SourcePipe(_) => old
              case other if oldState.forced.contains(other) => old
              case other =>
                val (st, a) = force(other, oldState)
                (st, a :: acts)
            }
          case ((oldState, acts), ToWrite.SimpleWrite(pipe, sink)) =>
            val (nextM, op) = plan(oldState.memo, pipe) // linter:disable:UndesirableTypeInference
            val action = () => {
              val arrayBufferF = op.result
              arrayBufferF.foreach { mem.writeSink(sink, _) }
              arrayBufferF.map(_ => ())
            }
            (oldState.copy(memo = nextM), action :: acts)
        }
        (nextState.copy(id = nextState.id + 1) , (nextState.id, acts))
      }
    // now we run the actions:
    Future.traverse(acts) { fn => fn() }.map(_ => (id, ExecutionCounters.empty))
  }

  /**
   * This should only be called after a call to execute
   */
  def getForced[T](
    conf: Config,
    mode: Mode,
    initial: TypedPipe[T]
    )(implicit cec: ConcurrentExecutionContext): Future[TypedPipe[T]] =
      state.get.forced.get(initial) match {
        case None => Future.failed(new Exception(s"$initial not forced"))
        case Some(f) => f.asInstanceOf[Future[TypedPipe[T]]]
      }

  /**
   * This should only be called after a call to execute
   */
  def getIterable[T](
    conf: Config,
    mode: Mode,
    initial: TypedPipe[T]
  )(implicit cec: ConcurrentExecutionContext): Future[Iterable[T]] = initial match {
    case TypedPipe.EmptyTypedPipe => Future.successful(Nil)
    case TypedPipe.IterablePipe(iter) => Future.successful(iter)
    case TypedPipe.SourcePipe(src) => mem.readSource(src) match {
      case Some(iter) => Future.successful(iter)
      case None => Future.failed(new Exception(s"Source: $src not connected"))
    }
    case other => getForced(conf, mode, other).flatMap(getIterable(conf, mode, _))
  }
}
