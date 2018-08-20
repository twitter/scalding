package com.twitter.scalding.typed.memory_backend

import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext, Promise }
import com.stripe.dagon.{ HMap, Rule }
import com.twitter.scalding.typed._
import com.twitter.scalding.{ Config, Execution, ExecutionCounters }
import Execution.{ ToWrite, Writer }
/**
 * This is the state of a single outer Execution execution running
 * in memory mode
 */
class MemoryWriter(mem: MemoryMode) extends Writer {

  def start(): Unit = ()
  /**
   * This is called by an Execution to end processing
   */
  def finished(): Unit = ()

  private[this] case class State(
    id: Long,
    forced: HMap[TypedPipe, ({ type F[T] = Future[Iterable[T]] })#F]) {

    def simplifiedForce[A](t: TypedPipe[A], it: Future[Iterable[A]]): State =
      copy(forced = forced.updated(t, it))
  }

  private[this] val state = new AtomicBox[State](State(0, HMap.empty[TypedPipe, ({ type F[T] = Future[Iterable[T]] })#F]))

  /**
   * do a batch of writes, possibly optimizing, and return a new unique
   * Long.
   *
   * empty writes are legitmate and should still return a Long
   */
  def execute(
    conf: Config,
    writes: List[ToWrite[_]])(implicit cec: ConcurrentExecutionContext): Future[(Long, ExecutionCounters)] = {

    val planner = MemoryPlanner.planner(conf, mem.srcs)

    type Action = () => Future[Unit]
    import Execution.ToWrite._

    val phases: Seq[Rule[TypedPipe]] =
      OptimizationRules.standardMapReduceRules // probably want to tweak this

    val optimizedWrites = ToWrite.optimizeWriteBatch(writes, phases)

    def force[T](p: TypedPipe[T], keyPipe: TypedPipe[T], oldState: State): (State, Action) = {
      val pipePromise = Promise[Iterable[T]]()
      val action = () => {
        val op = planner(p)
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
      val (nextState, acts) = optimizedWrites.foldLeft((s, List.empty[Action])) {
        case (old @ (state, acts), write) =>
          write match {
            case OptimizedWrite(pipe, Force(opt)) =>
              if (state.forced.contains(opt)) old
              else {
                val (st, a) = force(opt, pipe, state)
                (st, a :: acts)
              }
            case OptimizedWrite(pipe, ToIterable(opt)) =>
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
            case OptimizedWrite(pipe, ToWrite.SimpleWrite(opt, sink)) =>
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
      (nextState.copy(id = nextState.id + 1), (nextState.id, acts))
    }
    // now we run the actions:
    Future.traverse(acts) { fn => fn() }.map(_ => (id, ExecutionCounters.empty))
  }

  /**
   * This should only be called after a call to execute
   */
  def getForced[T](
    conf: Config,
    initial: TypedPipe[T])(implicit cec: ConcurrentExecutionContext): Future[TypedPipe[T]] =
    state.get.forced.get(initial) match {
      case None => Future.failed(new Exception(s"$initial not forced"))
      case Some(f) => f.map(TypedPipe.from(_))
    }

  private def getSource[A](src: TypedSource[A])(implicit cec: ConcurrentExecutionContext): Future[Iterable[A]] =
    MemorySource.readOption(mem.srcs(src), src.toString).map(_.toList)

  /**
   * This should only be called after a call to execute
   */
  def getIterable[T](
    conf: Config,
    initial: TypedPipe[T])(implicit cec: ConcurrentExecutionContext): Future[Iterable[T]] = initial match {
    case TypedPipe.EmptyTypedPipe => Future.successful(Nil)
    case TypedPipe.IterablePipe(iter) => Future.successful(iter)
    case TypedPipe.SourcePipe(src) => getSource(src)
    case other => getForced(conf, other).flatMap(getIterable(conf, _))
  }
}
