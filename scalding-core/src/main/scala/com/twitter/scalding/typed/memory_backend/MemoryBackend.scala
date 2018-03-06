package com.twitter.scalding.typed.memory_backend

import cascading.flow.{ FlowDef, FlowConnector }
import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.{ Fields, TupleEntryIterator }
import com.stripe.dagon.{HMap, Rule, Memoize, FunctionK}
import com.twitter.scalding.typed._
import com.twitter.scalding.{ Config, Execution, ExecutionCounters, Mode }
import java.util.{ ArrayList, Collections, UUID }
import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext, Promise }
import scala.collection.mutable.{ ArrayBuffer, Map => MMap }
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import Execution.{ ToWrite, Writer }

final case class MemoryMode(srcs: Resolver[TypedSource, MemorySource], sinks: Resolver[TypedSink, MemorySink]) extends Mode {

  def newWriter(): Writer =
    new MemoryWriter(this)

  /**
   * Add a new source resolver whose sources take precedence over any currently registered
   * sources
   */
  def addSourceResolver(res: Resolver[TypedSource, MemorySource]): MemoryMode =
    MemoryMode(res.orElse(srcs), sinks)

  def addSource[T](src: TypedSource[T], ts: MemorySource[T]): MemoryMode =
    addSourceResolver(Resolver.pair(src, ts))

  def addSourceFn[T](src: TypedSource[T])(fn: ConcurrentExecutionContext => Future[Iterator[T]]): MemoryMode =
    addSource(src, MemorySource.Fn(fn))

  def addSourceIterable[T](src: TypedSource[T], iter: Iterable[T]): MemoryMode =
    addSource(src, MemorySource.FromIterable(iter))

  /**
   * Add a new sink resolver whose sinks take precedence over any currently registered
   * sinks
   */
  def addSinkResolver(res: Resolver[TypedSink, MemorySink]): MemoryMode =
    MemoryMode(srcs, res.orElse(sinks))

  def addSink[T](sink: TypedSink[T], msink: MemorySink[T]): MemoryMode =
    addSinkResolver(Resolver.pair(sink, msink))

  /**
   * This has a side effect of mutating the corresponding MemorySink
   */
  def writeSink[T](t: TypedSink[T], iter: Iterable[T])(implicit ec: ConcurrentExecutionContext): Future[Unit] =
    sinks(t) match {
      case Some(sink) => sink.write(iter)
      case None => Future.failed(new Exception(s"missing sink for $t, with first 10 values to write: ${iter.take(10).toList.toString}..."))
    }

  def readSource[T](t: TypedSource[T])(implicit ec: ConcurrentExecutionContext): Future[Iterator[T]] =
    srcs(t) match {
      case Some(src) => src.read()
      case None => Future.failed(new Exception(s"Source: $t not wired. Please provide an input with MemoryMode.addSource"))
    }
}

object MemoryMode {
  def empty: MemoryMode =
    apply(Resolver.empty[TypedSource, MemorySource], Resolver.empty[TypedSink, MemorySink])
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
   * This builds an new memoizing planner
   * that reads from the given MemoryMode.
   *
   * Note, this assumes all forks are made explicit
   * in the graph, so it is up to any caller
   * to make sure that optimization rule has first
   * been applied
   */
  def planner(mem: MemoryMode): FunctionK[TypedPipe, Op] =
    Memoize.functionK(new Memoize.RecursiveK[TypedPipe, Op] {
      import TypedPipe._

      def toFunction[T] = {
        case (CounterPipe(pipe), rec) =>
          // TODO: counters not yet supported, but can be with an concurrent hashmap
          rec(pipe.map(_._1))
        case (cp@CrossPipe(_, _), rec) =>
          rec(cp.viaHashJoin)
        case (CrossValue(left, EmptyValue), _) => Op.empty
        case (CrossValue(left, LiteralValue(v)), rec) =>
          val op = rec(left) // linter:disable:UndesirableTypeInference
          op.map((_, v))
        case (CrossValue(left, ComputedValue(right)), rec) =>
          rec(CrossPipe(left, right))
        case (DebugPipe(p), rec) =>
          // There is really little that can be done here but println
          rec(p.map { t => println(t); t })

        case (EmptyTypedPipe, _) =>
          // just use an empty iterable pipe.
          Op.empty[T]

        case (fk@FilterKeys(_, _), rec) =>
          def go[K, V](node: FilterKeys[K, V]): Op[(K, V)] = {
            val FilterKeys(pipe, fn) = node
            rec(pipe).concatMap { case (k, v) => if (fn(k)) { (k, v) :: Nil } else Nil }
          }
          go(fk)

        case (f@Filter(_, _), rec) =>
          def go[T](f: Filter[T]): Op[T] = {
            val Filter(p, fn) = f
            rec(p).filter(fn)
          }
          go(f)

        case (f@FlatMapValues(_, _), rec) =>
          def go[K, V, U](node: FlatMapValues[K, V, U]) = {
            val fn = node.fn
            rec(node.input).concatMap { case (k, v) => fn(v).map((k, _)) }
          }

          go(f)

        case (FlatMapped(prev, fn), rec) =>
          rec(prev).concatMap(fn) // linter:disable:UndesirableTypeInference

        case (ForceToDisk(pipe), rec) =>
          rec(pipe).materialize

        case (Fork(pipe), rec) =>
          rec(pipe).materialize

        case (IterablePipe(iterable), _) =>
          Op.source(iterable)

        case (f@MapValues(_, _), rec) =>
          def go[K, V, U](node: MapValues[K, V, U]) = {
            val mvfn = node.fn
            rec(node.input).map { case (k, v) => (k, mvfn(v)) }
          }

          go(f)

        case (Mapped(input, fn), rec) =>
          rec(input).map(fn) // linter:disable:UndesirableTypeInference

        case (MergedTypedPipe(left, right), rec) =>
          Op.Concat(rec(left), rec(right))

        case (SourcePipe(src), _) =>
          Op.Source({ cec => mem.readSource(src)(cec) })

        case (slk@SumByLocalKeys(_, _), rec) =>
          def sum[K, V](sblk: SumByLocalKeys[K, V]) = {
            val SumByLocalKeys(p, sg) = sblk

            rec(p).transform[(K, V), (K, V)] { kvs =>
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
            }
          }
          sum(slk)

        case (TrappedPipe(input, _, _), rec) =>
          // this can be interpretted as catching any exception
          // on the map-phase until the next partition, so it can
          // be made to work by changing Op to return all
          // the values that fail on error
          rec(input)

        case (WithDescriptionTypedPipe(pipe, descriptions), rec) =>
          // TODO we could optionally print out the descriptions
          // after the future completes
          rec(pipe)

        case (WithOnComplete(pipe, fn), rec) =>
          Op.OnComplete(rec(pipe), fn)

        case (hcg@HashCoGroup(_, _, _), rec) =>
          def go[K, V1, V2, R](hcg: HashCoGroup[K, V1, V2, R]) = {
            val leftOp = rec(hcg.left)
            val rightOp = rec(ReduceStepPipe(HashJoinable.toReduceStep(hcg.right)))
            Op.Join[(K, V1), (K, V2), (K, R)](leftOp, rightOp, { (v1s, v2s) =>
              val kv2 = v2s.groupBy(_._1)
              val result = new ArrayBuffer[(K, R)]()
              v1s.foreach { case (k, v1) =>
                val v2 = kv2.getOrElse(k, Nil).map(_._2)
                result ++= hcg.joiner(k, v1, v2).map((k, _))
              }
              result
            })
          }
          go(hcg)

        case (CoGroupedPipe(cg), rec) =>
          def go[K, V](cg: CoGrouped[K, V]) = {
            Op.BulkJoin(cg.inputs.map(rec(_)), cg.joinFunction)
          }
          go(cg)

        case (ReduceStepPipe(ir@IdentityReduce(_, _, _, descriptions, _)), rec) =>
          def go[K, V1, V2](ir: IdentityReduce[K, V1, V2]): Op[(K, V2)] = {
            type OpT[V] = Op[(K, V)]
            val op = rec(ir.mapped)
            ir.evidence.subst[OpT](op)
          }
          go(ir)
        case (ReduceStepPipe(uir@UnsortedIdentityReduce(_, _, _, descriptions, _)), rec) =>
          def go[K, V1, V2](uir: UnsortedIdentityReduce[K, V1, V2]): Op[(K, V2)] = {
            type OpT[V] = Op[(K, V)]
            val op = rec(uir.mapped)
            uir.evidence.subst[OpT](op)
          }
          go(uir)
        case (ReduceStepPipe(IdentityValueSortedReduce(_, pipe, ord, _, _, _)), rec) =>
          def go[K, V](p: TypedPipe[(K, V)], ord: Ordering[V]) = {
            val op = rec(p)
            Op.Reduce[K, V, V](op, { (k, vs) => vs }, Some(ord))
          }
          go(pipe, ord)
        case (ReduceStepPipe(ValueSortedReduce(_, pipe, ord, fn, _, _)), rec) =>
          Op.Reduce(rec(pipe), fn, Some(ord))
        case (ReduceStepPipe(IteratorMappedReduce(_, pipe, fn, _, _)), rec) =>
          Op.Reduce(rec(pipe), fn, None)
      }
    })

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

  import MemoryPlanner.Op

  def start(): Unit = ()
  /**
   * This is called by an Execution to end processing
   */
  def finished(): Unit = ()

  private[this] case class State(
    id: Long,
    forced: HMap[TypedPipe, ({type F[T]=Future[Iterable[T]]})#F]
    ) {

    def simplifiedForce[A](t: TypedPipe[A], it: Future[Iterable[A]]): State =
      copy(forced = forced.updated(t, it))
  }

  private[this] val state = new AtomicBox[State](State(0, HMap.empty[TypedPipe, ({type F[T]=Future[Iterable[T]]})#F]))

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

      val planner = MemoryPlanner.planner(mem)

      type Action = () => Future[Unit]
      import Execution.ToWrite._

      val phases: Seq[Rule[TypedPipe]] =
        OptimizationRules.standardMapReduceRules // probably want to tweak this

      val toOptimized = ToWrite.optimizeWriteBatch(writes, phases)

      def force[T](p: TypedPipe[T], keyPipe: TypedPipe[T], oldState: State): (State, Action) = {
        val op = planner(p)
        val pipePromise = Promise[Iterable[T]]()
        val action = () => {
          val arrayBufferF = op.result
          pipePromise.completeWith(arrayBufferF)

          arrayBufferF.map(_ => ())
        }
        (oldState.copy(forced = oldState.forced.updated(keyPipe, pipePromise.future)), action)
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
                  val op = planner(opt) // linter:disable:UndesirableTypeInference
                  val action = () => {
                    val arrayBufferF = op.result
                    arrayBufferF.flatMap(mem.writeSink(sink, _))
                  }
                  (state, action :: acts)
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
