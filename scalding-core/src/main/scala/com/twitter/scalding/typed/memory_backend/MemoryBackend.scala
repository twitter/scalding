package com.twitter.scalding.typed.memory_backend

import cascading.flow.{ FlowDef, FlowConnector }
import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.{ Fields, TupleEntryIterator }
import com.stripe.dagon.{HMap, Rule}
import com.twitter.scalding.typed._
import com.twitter.scalding.{ Config, Execution, ExecutionCounters, Mode }
import java.util.{ ArrayList, Collections, UUID }
import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext, Promise }
import scala.collection.mutable.{ ArrayBuffer, Map => MMap }
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import Execution.{ ToWrite, Writer }

final class MemoryMode private (srcs: HMap[TypedSource, MemorySource], sinks: HMap[TypedSink, MemorySink]) extends Mode {

  def newWriter(): Writer =
    new MemoryWriter(this)

  def addSource[T](src: TypedSource[T], ts: MemorySource[T]): MemoryMode =
    new MemoryMode(srcs + (src -> ts), sinks)

  def addSourceFn[T](src: TypedSource[T])(fn: ConcurrentExecutionContext => Future[Iterator[T]]): MemoryMode =
    new MemoryMode(srcs + (src -> MemorySource.Fn(fn)), sinks)

  def addSourceIterable[T](src: TypedSource[T], iter: Iterable[T]): MemoryMode =
    new MemoryMode(srcs + (src -> MemorySource.FromIterable(iter)), sinks)

  def addSink[T](sink: TypedSink[T], msink: MemorySink[T]): MemoryMode =
    new MemoryMode(srcs, sinks + (sink -> msink))

  /**
   * This has a side effect of mutating this MemoryMode
   */
  def writeSink[T](t: TypedSink[T], iter: Iterable[T])(implicit ec: ConcurrentExecutionContext): Future[Unit] =
    sinks.get(t) match {
      case Some(sink) => sink.write(iter)
      case None => Future.failed(new Exception(s"missing sink for $t, with first 10 values to write: ${iter.take(10).toList.toString}..."))
    }

  def readSource[T](t: TypedSource[T])(implicit ec: ConcurrentExecutionContext): Future[Iterator[T]] =
    srcs.get(t) match {
      case Some(src) => src.read()
      case None => Future.failed(new Exception(s"Source: $t not wired. Please provide an input with MemoryMode.addSource"))
    }
}

object MemoryMode {
  def empty: MemoryMode = new MemoryMode(HMap.empty, HMap.empty)
}

trait MemorySource[A] {
  def read()(implicit ec: ConcurrentExecutionContext): Future[Iterator[A]]
}

object MemorySource {
  case class FromIterable[A](iter: Iterable[A]) extends MemorySource[A] {
    def read()(implicit ec: ConcurrentExecutionContext) = Future.successful(iter.iterator)
  }
  case class Fn[A](toFn: ConcurrentExecutionContext => Future[Iterator[A]]) extends MemorySource[A] {
    def read()(implicit ec: ConcurrentExecutionContext) = toFn(ec)
  }
}

trait MemorySink[A] {
  def write(data: Iterable[A])(implicit ec: ConcurrentExecutionContext): Future[Unit]
}

object MemorySink {
  /**
   * This is a sink that writes into local memory which you can read out
   * by a future
   *
   * this needs to be reset between each write (so it only works for a single
   * write per Execution)
   */
  class LocalVar[A] extends MemorySink[A] {
    private[this] val box: AtomicBox[Promise[Iterable[A]]] = new AtomicBox(Promise[Iterable[A]]())

    /**
     * This is a future that completes when a write comes. If no write
     * happens before a reset, the future fails
     */
    def read(): Future[Iterable[A]] = box.get().future

    /**
     * This takes the current future and resets the promise
     * making it safe for another write.
     */
    def reset(): Option[Iterable[A]] = {
      val current = box.swap(Promise[Iterable[A]]())
      // if the promise is not set, it never will be, so
      // go ahead and poll now
      //
      // also note we never set this future to failed
      current.future.value match {
        case Some(Success(res)) =>
          Some(res)
        case Some(Failure(err)) =>
          throw new IllegalStateException("We should never reach this because, we only complete with failure below", err)
        case None =>
          // make sure we complete the original future so readers don't block forever
          current.failure(new Exception(s"sink never written to before reset() called $this"))
          None
      }
    }

    def write(data: Iterable[A])(implicit ec: ConcurrentExecutionContext): Future[Unit] =
      Future {
        box.update { p => (p.success(data), ()) }
      }
  }
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
      Op.MapOp(this, fn)

    def filter(fn: O => Boolean): Op[O] =
      Op.Filter(this, fn)

    def transform[O1 >: O, O2](fn: IndexedSeq[O1] => ArrayBuffer[O2]): Op[O2] =
      Op.Transform[O1, O2](this, fn)

    def materialize: Op[O] =
      Op.Materialize(this)
  }
  object Op {
    def source[I](i: Iterable[I]): Op[I] = Source(_ => Future.successful(i.iterator))
    def empty[I]: Op[I] = source(Nil)

    final case class Source[I](input: ConcurrentExecutionContext => Future[Iterator[I]]) extends Op[I] {

      def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[I]] =
        input(cec).map(ArrayBuffer.empty[I] ++= _)
    }

    // Here we need to make a copy on each result
    final case class Materialize[O](op: Op[O]) extends Op[O] {
      private[this] val promiseBox: AtomicBox[Option[Promise[ArrayBuffer[_ <: O]]]] = new AtomicBox(None)

      def result(implicit cec: ConcurrentExecutionContext) = {
        val either = promiseBox.update {
          case None =>
            val promise = Promise[ArrayBuffer[_ <: O]]()
            (Some(promise), Right(promise))
          case s@Some(promise) =>
            (s, Left(promise))
        }

        val fut = either match {
          case Right(promise) =>
            // This is the one case where we call the op
            promise.completeWith(op.result)
            promise.future
          case Left(promise) =>
            // we already started the previous work
            promise.future
        }
        fut.map(ArrayBuffer.concat(_))
      }
    }

    final case class Concat[O](left: Op[O], right: Op[O]) extends Op[O] {
      def result(implicit cec: ConcurrentExecutionContext) = {
        val f1 = left.result
        val f2 = right.result
        f1.zip(f2).map { case (l, r) =>
          if (l.size > r.size) l.asInstanceOf[ArrayBuffer[O]] ++= r
          else r.asInstanceOf[ArrayBuffer[O]] ++= l
        }
      }
    }

    // We reuse the input on map
    final case class MapOp[I, O](input: Op[I], fn: I => O) extends Op[O] {
      def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[O]] =
        input.result.map { array =>
          val res: ArrayBuffer[O] = array.asInstanceOf[ArrayBuffer[O]]
          var pos = 0
          while(pos < array.length) {
            res.update(pos, fn(array(pos)))
            pos = pos + 1
          }
          res
        }
    }
    // We reuse the input on filter
    final case class Filter[I](input: Op[I], fn: I => Boolean) extends Op[I] {
      def result(implicit cec: ConcurrentExecutionContext): Future[ArrayBuffer[I]] =
        input.result.map { array0 =>
          val array = array0.asInstanceOf[ArrayBuffer[I]]
          var pos = 0
          var writePos = 0
          while(pos < array.length) {
            val item = array(pos)
            if (fn(item)) {
              array(writePos) = item
              writePos = writePos + 1
            }
            pos = pos + 1
          }
          // trim the tail off
          array.remove(writePos, array.length - writePos)
          array
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
      ord: Option[Ordering[V1]]
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

    final case class BulkJoin[K, A](ops: List[Op[(K, Any)]], joinF: MultiJoinFunction[K, A]) extends Op[(K, A)] {
      def result(implicit cec: ConcurrentExecutionContext) =
        Future.traverse(ops)(_.result)
          .map { items =>
            // TODO this is not by any means optimal.
            // we could copy into arrays then sort by key and iterate
            // each through in K sorted order
            val maps: List[Map[K, Iterable[(K, Any)]]] = items.map { kvs =>
              val kvMap: Map[K, Iterable[(K, Any)]] = kvs.groupBy(_._1)
              kvMap
            }

            val allKeys = maps.iterator.flatMap(_.keys.iterator).toSet
            val result = ArrayBuffer[(K, A)]()
            allKeys.foreach { k =>
              maps.map(_.getOrElse(k, Nil)) match {
                case h :: tail =>
                  joinF(k, h.iterator.map(_._2), tail.map(_.map(_._2))).foreach { a =>
                    result += ((k, a))
                  }
                case other => sys.error(s"unreachable: $other, $k")
              }
            }

            result
          }
    }
  }

  /**
   * Memoize previously planned TypedPipes so we don't replan too much
   */
  case class Memo(planned: HMap[TypedPipe, Op]) {
    def plan[T](t: TypedPipe[T])(op: => (Memo, Op[T])): (Memo, Op[T]) =
      planned.get(t) match {
        case Some(op) => (this, op)
        case None =>
          val (m1, newOp) = op
          (m1.copy(planned = m1.planned.updated(t, newOp)), newOp)
      }
  }

  object Memo {
    val empty: Memo = Memo(HMap.empty)
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

/**
 * This is the state of a single outer Execution execution running
 * in memory mode
 */
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
            val (m1, op) = plan(m, p)
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
          (m1, op.materialize)

        case Fork(pipe) =>
          val (m1, op) = plan(m, pipe)
          (m1, op.materialize)

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
          (m, Op.Source({ cec => mem.readSource(src)(cec) }))

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

        case tp@TrappedPipe(input, _, _) => plan(m, input)
          // this can be interpretted as catching any exception
          // on the map-phase until the next partition, so it can
          // be made to work by changing Op to return all
          // the values that fail on error

        case WithDescriptionTypedPipe(pipe, descriptions) =>
          // TODO we could optionally print out the descriptions
          // after the future completes
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
            val (m2, opsRev) = inputs.foldLeft((m, List.empty[Op[(K, Any)]])) { case ((oldM, ops), pipe) =>
              val (m1, op) = plan(oldM, pipe)
              (m1, op :: ops)
            }
            (m2, Op.BulkJoin(opsRev.reverse, joinf))
          }
          go(cg)

        case ReduceStepPipe(ir@IdentityReduce(_, _, _, descriptions, _)) =>
          def go[K, V1, V2](ir: IdentityReduce[K, V1, V2]): (Memo, Op[(K, V2)]) = {
            type OpT[V] = Op[(K, V)]
            val (m1, op) = plan(m, ir.mapped)
            (m1, ir.evidence.subst[OpT](op))
          }
          go(ir)
        case ReduceStepPipe(uir@UnsortedIdentityReduce(_, _, _, descriptions, _)) =>
          def go[K, V1, V2](uir: UnsortedIdentityReduce[K, V1, V2]): (Memo, Op[(K, V2)]) = {
            type OpT[V] = Op[(K, V)]
            val (m1, op) = plan(m, uir.mapped)
            (m1, uir.evidence.subst[OpT](op))
          }
          go(uir)
        case ReduceStepPipe(IdentityValueSortedReduce(_, pipe, ord, _, _, _)) =>
          def go[K, V](p: TypedPipe[(K, V)], ord: Ordering[V]) = {
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
    case ir@IdentityReduce(_, _, _, _, _) =>
      type OpT[V] = Op[(K, V)]
      val (m1, op) = plan(m, ir.mapped)
      (m1, ir.evidence.subst[OpT](op))
    case uir@UnsortedIdentityReduce(_, _, _, _, _) =>
      type OpT[V] = Op[(K, V)]
      val (m1, op) = plan(m, uir.mapped)
      (m1, uir.evidence.subst[OpT](op))
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
    forced: HMap[TypedPipe, ({type F[T]=Future[Iterable[T]]})#F]
    ) {

    def simplifiedForce[A](t: TypedPipe[A], it: Future[Iterable[A]]): State =
      copy(forced = forced.updated(t, it))
  }

  private[this] val state = new AtomicBox[State](State(0, MemoryPlanner.Memo.empty, HMap.empty[TypedPipe, ({type F[T]=Future[Iterable[T]]})#F]))

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
      import Execution.ToWrite._

      val phases: Seq[Rule[TypedPipe]] =
        OptimizationRules.standardMapReduceRules // probably want to tweak this

      val toOptimized = ToWrite.optimizeWriteBatch(writes, phases)

      def force[T](p: TypedPipe[T], keyPipe: TypedPipe[T], oldState: State): (State, Action) = {
        val (nextM, op) = plan(oldState.memo, p)
        val pipePromise = Promise[Iterable[T]]()
        val action = () => {
          val arrayBufferF = op.result
          pipePromise.completeWith(arrayBufferF)

          arrayBufferF.map(_ => ())
        }
        (oldState.copy(memo = nextM, forced = oldState.forced.updated(keyPipe, pipePromise.future)), action)
      }

      /**
       * TODO
       * If we have a typed pipe rooted twice, it is not clear it has fanout. If it does not
       * we will not materialize it, so both branches can't own it. Since we only emit Iterable
       * out, this may be okay because no external readers can modify, but worth thinking of
       */
      val (id, acts) = state.update { s =>
        val (nextState, acts) = writes.foldLeft((s, List.empty[Action])) { case (old@(state, acts), write) =>
          write match {
            case Force(pipe) =>
              val opt = toOptimized(pipe)
              if (state.forced.contains(opt)) old
              else {
                val (st, a) = force(opt, pipe, state)
                (st, a :: acts)
              }
            case ToIterable(pipe) =>
              val opt = toOptimized(pipe)
              opt match {
                case TypedPipe.EmptyTypedPipe =>
                  (state.simplifiedForce(pipe, Future.successful(Nil)), acts)
                case TypedPipe.IterablePipe(i) =>
                  (state.simplifiedForce(pipe, Future.successful(i)), acts)
                case TypedPipe.SourcePipe(src) =>
                  val fut = getSource(src)
                  (state.simplifiedForce(pipe, fut), acts)
                case other if state.forced.contains(opt) => old
                case other =>
                  val (st, a) = force(opt, pipe, state)
                  (st, a :: acts)
              }
            case ToWrite.SimpleWrite(pipe, sink) =>
              val opt = toOptimized(pipe)
              state.forced.get(opt) match {
                case Some(iterf) =>
                  val action = () => {
                    iterf.flatMap(mem.writeSink(sink, _))
                  }
                  (state, action :: acts)
                case None =>
                  val (nextM, op) = plan(state.memo, opt) // linter:disable:UndesirableTypeInference
                  val action = () => {
                    val arrayBufferF = op.result
                    arrayBufferF.flatMap(mem.writeSink(sink, _))
                  }
                  (state.copy(memo = nextM), action :: acts)
              }
          }
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
        case Some(f) => f.map(TypedPipe.from(_))
      }

  private def getSource[A](src: TypedSource[A])(implicit cec: ConcurrentExecutionContext): Future[Iterable[A]] =
    mem.readSource(src).map(_.toList)

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
    case TypedPipe.SourcePipe(src) => getSource(src)
    case other => getForced(conf, mode, other).flatMap(getIterable(conf, mode, _))
  }
}
