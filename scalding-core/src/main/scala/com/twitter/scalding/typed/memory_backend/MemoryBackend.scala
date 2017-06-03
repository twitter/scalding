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

    def flatMap[O1](fn: O => TraversableOnce[O1]): Op[O1] =
      Op.Map(this, fn)

    def mapAll[O1 >: O, O2](fn: IndexedSeq[O1] => ArrayBuffer[O2]): Op[O2] =
      Op.MapAll[O1, O2](this, fn)
  }
  object Op {
    def source[I](i: Iterable[I]): Op[I] = Source(_ => Future.successful(i))
    def empty[I]: Op[I] = source(Nil)

    case class Source[I](input: ConcurrentExecutionContext => Future[Iterable[I]]) extends Op[I] {
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

    case class Materialize[O](op: Op[O]) extends Op[O] {
      private[this] val promise: Promise[ArrayBuffer[_ <: O]] = Promise()

      def result(implicit cec: ConcurrentExecutionContext) = {
        if (!promise.isCompleted) {
          promise.tryCompleteWith(op.result)
        }

        promise.future
      }
    }

    case class Concat[O](left: Op[O], right: Op[O]) extends Op[O] {
      def result(implicit cec: ConcurrentExecutionContext) =
        for {
          l <- left.result
          r <- right.result
        } yield {
          ArrayBuffer.concat(l, r)
        }
    }

    case class Map[I, O](input: Op[I], fn: I => TraversableOnce[O]) extends Op[O] {
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

    case class MapAll[I, O](input: Op[I], fn: IndexedSeq[I] => ArrayBuffer[O]) extends Op[O] {
      def result(implicit cec: ConcurrentExecutionContext) =
        input.result.map(fn)
    }

    case class Reduce[K, V1, V2](
      input: Op[(K, V1)],
      fn: (K, Iterator[V1]) => Iterator[V2],
      ord: Option[Ordering[_ >: V1]]
      ) extends Op[(K, V2)] {

      def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[(K, V2)]] =
        input.result.map { kvs =>
          val m = MMap[K, ArrayList[V1]]()
          def add(kv: (K, V1)): Unit = {
            val vs = m.getOrElseUpdate(kv._1, new ArrayList[V1]())
            vs.add(kv._2)
          }
          kvs.foreach(add)

          /*
           * This portion could be parallelized for each key, or we could split
           * the keys into as many groups as there are CPUs and process that way
           */
          val res = ArrayBuffer[(K, V2)]()
          m.foreach { case (k, vs) =>
            ord.foreach(Collections.sort[V1](vs, _))
            val v2iter = fn(k, vs.iterator.asScala)
            while(v2iter.hasNext) {
              res += ((k, v2iter.next))
            }
          }
          res
      }
    }

    case class Join[A, B, C](
      opA: Op[A],
      opB: Op[B],
      fn: (IndexedSeq[A], IndexedSeq[B]) => ArrayBuffer[C]) extends Op[C] {

      def result(implicit cec: ConcurrentExecutionContext) =
        for {
          as <- opA.result
          bs <- opB.result
        } yield fn(as, bs)
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
  def converter[U >: T] = ???
  def read(implicit flowDef: FlowDef, mode: Mode): Pipe = ???
}

/**
 * These are just used as type markers which are connected
 * to outputs via the MemoryMode
 */
case class SinkT[T](indent: String) extends TypedSink[T] {
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
        case cp@CrossPipe(_, _) =>
          plan(m, cp.viaHashJoin)

        case cv@CrossValue(_, _) =>
          plan(m, cv.viaHashJoin)

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
            (m1, op.flatMap { case (k, v) => if (fn(k)) { (k, v) :: Nil } else Nil })
          }
          go(fk)

        case f@Filter(_, _) =>
          def go[T](f: Filter[T]): (Memo, Op[T]) = {
            val Filter(p, fn) = f
            val (m1, op) = plan(m, f)
            (m1, op.flatMap { t => if (fn(t)) t :: Nil else Nil })
          }
          go(f)

        case f@FlatMapValues(_, _) =>
          def go[K, V, U](node: FlatMapValues[K, V, U]) = {
            // don't capture node, which is a TypedPipe, which we avoid serializing
            val fn = node.fn
            val (m1, op) = plan(m, node.input)
            (m1, op.flatMap { case (k, v) => fn(v).map((k, _)) })
          }

          go(f)

        case FlatMapped(prev, fn) =>
          val (m1, op) = plan(m, prev)
          (m1, op.flatMap(fn))

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
            // don't capture node, which is a TypedPipe, which we avoid serializing
            val mvfn = node.fn
            val (m1, op) = plan(m, node.input)
            (m1, op.flatMap { case (k, v) => Iterator.single((k, mvfn(v))) })
          }

          go(f)

        case Mapped(input, fn) =>
          val (m1, op) = plan(m, input)
          (m1, op.flatMap { t => fn(t) :: Nil })

        case MergedTypedPipe(left, right) =>
          val (m1, op1) = plan(m, left)
          val (m2, op2) = plan(m1, right)
          (m2, Op.Concat(op1, op2))

        case SourcePipe(src) =>
          (m, Op.Source({ cec =>
            mem.readSource(src) match {
              case Some(iter) => Future.successful(iter)
              case None => Future.failed(new Exception(s"Source: $src not wired"))
            }
          }))

        case slk@SumByLocalKeys(_, _) =>
          def sum[K, V](sblk: SumByLocalKeys[K, V]) = {
            val SumByLocalKeys(p, sg) = sblk

            val (m1, op) = plan(m, p)
            (m1, op.mapAll[(K, V), (K, V)] { kvs =>
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

        case WithOnComplete(pipe, fn) => ???

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

        case cgp@CoGroupedPipe(_) => ???

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

  def planHashJoinable[K, V](m: Memo, hk: HashJoinable[K, V]): (Memo, Op[(K, V)]) = ???

  case class State(
    id: Long,
    memo: MemoryPlanner.Memo,
    forced: Map[TypedPipe[_], Future[TypedPipe[_]]]
    )

  val state = new AtomicBox[State](State(0, MemoryPlanner.Memo.empty, Map.empty))

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
            val (nextM, op) = plan(oldState.memo, pipe)
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
