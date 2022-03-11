package com.twitter.scalding.typed.cascading_backend

import cascading.flow.{Flow, FlowDef, FlowConnector}
import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.{Fields, TupleEntryIterator}
import com.twitter.scalding.cascading_interop.FlowListenerPromise
import com.twitter.scalding.filecache.{CachedFile, DistributedCacheFile}
import org.apache.hadoop.conf.Configuration
import scala.concurrent.Future
import scala.util.Try

import com.twitter.scalding._

import TupleConverter.singleConverter
import scala.collection.JavaConverters._

trait CascadingExtensions {
  implicit class TypedPipeCompanionCascadingExtensions(tp: TypedPipe.type) {
    /**
     * Create a TypedPipe from a cascading Pipe, some Fields and the type T Avoid this if you can. Prefer
     * from(TypedSource).
     */
    def fromPipe[T](pipe: Pipe, fields: Fields)(implicit
        flowDef: FlowDef,
        mode: Mode,
        conv: TupleConverter[T]
    ): TypedPipe[T] = {

        /*
        * This could be in TypedSource, but we don't want to encourage users
        * to work directly with Pipe
        */
        case class WrappingSource[T](
            pipe: Pipe,
            fields: Fields,
            @transient localFlow: FlowDef, // FlowDef is not serializable. We shouldn't need to, but being paranoid
            mode: Mode,
            conv: TupleConverter[T]
        ) extends TypedSource[T] {

        def converter[U >: T]: TupleConverter[U] =
            TupleConverter.asSuperConverter[T, U](conv)

        def read(implicit fd: FlowDef, m: Mode): Pipe = {
            // This check is not likely to fail unless someone does something really strange.
            // for historical reasons, it is not checked by the typed system
            require(
            m == mode,
            s"Cannot switch Mode between TypedPipe.from and toPipe calls. Pipe: $pipe, pipe mode: $m, outer mode: $mode"
            )
            Dsl.flowDefToRichFlowDef(fd).mergeFrom(localFlow)
            pipe
        }

        override def sourceFields: Fields = fields
        }

        val localFlow = Dsl.flowDefToRichFlowDef(flowDef).onlyUpstreamFrom(pipe)
        TypedPipe.from(WrappingSource(pipe, fields, localFlow, mode, conv))
    }

    /**
     * Input must be a Pipe with exactly one Field Avoid this method and prefer from(TypedSource) if possible
     */
    def fromSingleField[T](pipe: Pipe)(implicit fd: FlowDef, mode: Mode): TypedPipe[T] =
        fromPipe(pipe, new Fields(0))(fd, mode, singleConverter[T])

  }

  implicit class TypedPipeCascadingExtensions[T](pipe: TypedPipe[T]) {
    /**
     * Export back to a raw cascading Pipe. useful for interop with the scalding Fields API or with Cascading
     * code. Avoid this if possible. Prefer to write to TypedSink.
     */
    final def toPipe[U >: T](
        fieldNames: Fields
    )(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe =
        // we have to be cafeful to pass the setter we want since a low priority implicit can always be
        // found :(
        CascadingBackend.toPipe[U](pipe.withLine, fieldNames)(flowDef, mode, setter)

    /** use a TupleUnpacker to flatten U out into a cascading Tuple */
    def unpackToPipe[U >: T](
        fieldNames: Fields
    )(implicit fd: FlowDef, mode: Mode, up: TupleUnpacker[U]): Pipe = {
        val setter = up.newSetter(fieldNames)
        toPipe[U](fieldNames)(fd, mode, setter)
    }

    /**
     * If you want to writeThrough to a specific file if it doesn't already exist, and otherwise just read from
     * it going forward, use this.
     */
    def make[U >: T](dest: Source with TypedSink[T] with TypedSource[U]): Execution[TypedPipe[U]] =
        Execution.getMode.flatMap { mode =>
        try {
            dest.validateTaps(mode)
            Execution.from(TypedPipe.from(dest))
        } catch {
            case ivs: InvalidSourceException => pipe.writeThrough(dest)
        }
    }

    /**
     * Safely write to a TypedSink[T]. If you want to write to a Source (not a Sink) you need to do something
     * like: toPipe(fieldNames).write(dest)
     * @return
     *   a pipe equivalent to the current pipe.
     */
    def write(dest: TypedSink[T])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] = {
        // We do want to record the line number that this occurred at
        val next = pipe.withLine
        FlowStateMap.merge(flowDef, FlowState.withTypedWrite(next, dest, mode))
        next
    }
  }

  implicit class ExecutionCompanionCascadingExtensions(ex: Execution.type) {
    def fromFn(fn: (Config, Mode) => FlowDef): Execution[Unit] = ???

    /**
     * Distributes the file onto each map/reduce node, so you can use it for Scalding source creation and
     * TypedPipe, KeyedList, etc. transformations. Using the [[com.twitter.scalding.filecache.CachedFile]]
     * outside of Execution will probably not work.
     *
     * For multiple files you must nested your execution, see docs of
     * [[com.twitter.scalding.filecache.DistributedCacheFile]]
     */
     def withCachedFile[T](path: String)(fn: CachedFile => Execution[T]): Execution[T] =
        Execution.getMode.flatMap { mode =>
        val cachedFile = DistributedCacheFile.cachedFile(path, mode)

        withConfig(fn(cachedFile))(_.addDistributedCacheFiles(cachedFile))
    }

    /*
    * This runs a Flow using Cascading's built in threads. The resulting JobStats
    * are put into a promise when they are ready
    */
    def run[C](flow: Flow[C]): Future[JobStats] =
        // This is in Java because of the cascading API's raw types on FlowListener
        FlowListenerPromise.start(flow, { f: Flow[C] => JobStats(f.getFlowStats) })
    private def run[L, C](label: L, flow: Flow[C]): Future[(L, JobStats)] =
        // This is in Java because of the cascading API's raw types on FlowListener
        FlowListenerPromise.start(flow, { f: Flow[C] => (label, JobStats(f.getFlowStats)) })

    /*
    * This blocks the current thread until the job completes with either success or
    * failure.
    */
    def waitFor[C](flow: Flow[C]): Try[JobStats] =
        Try {
        flow.complete()
        JobStats(flow.getStats)
        }

  }

  @deprecated(
    "Use CascadingMode.cast(mode) or pattern match directly on known CascadingModes (e.g. Hdfs, Local)",
    "0.18.0"
  )
  implicit class DeprecatedCascadingModeExtensions(mode: Mode) {
    private def cmode: CascadingMode = CascadingMode.cast(mode)

    def openForRead(config: Config, tap: Tap[_, _, _]): TupleEntryIterator =
      cmode.openForRead(config, tap)

    def openForRead(tap: Tap[_, _, _]): TupleEntryIterator =
      openForRead(Config.defaultFrom(mode), tap)

    // Returns true if the file exists on the current filesystem.
    def fileExists(filename: String): Boolean =
      cmode.fileExists(filename)

    /** Create a new FlowConnector for this cascading planner */
    def newFlowConnector(props: Config): FlowConnector =
      cmode.newFlowConnector(props)
  }

  implicit class ModeCompanionExtensions(m: Mode.type) {
    def CascadingFlowConnectorClassKey = "cascading.flow.connector.class"
    def CascadingFlowProcessClassKey = "cascading.flow.process.class"

    def DefaultHadoopFlowConnector = "cascading.flow.hadoop.HadoopFlowConnector"
    def DefaultHadoopFlowProcess = "cascading.flow.hadoop.HadoopFlowProcess"

    def DefaultHadoop2Mr1FlowConnector = "cascading.flow.hadoop2.Hadoop2MR1FlowConnector"
    def DefaultHadoop2Mr1FlowProcess =
        "cascading.flow.hadoop.HadoopFlowProcess" // no Hadoop2MR1FlowProcess as of Cascading 3.0.0-wip-75?

    def DefaultHadoop2TezFlowConnector = "cascading.flow.tez.Hadoop2TezFlowConnector"
    def DefaultHadoop2TezFlowProcess = "cascading.flow.tez.Hadoop2TezFlowProcess"

    // This should be passed ALL the args supplied after the job name
    def apply(args: Args, config: Configuration): Mode = {
        val strictSources = args.boolean("tool.partialok") == false
        if (!strictSources) {
        // TODO we should do smarter logging here
        println("[Scalding:INFO] using --tool.partialok. Missing log data won't cause errors.")
        }

        if (args.boolean("local"))
        Local(strictSources)
        else if (
        args.boolean("hdfs")
        ) /* FIXME: should we start printing deprecation warnings ? It's okay to set manually c.f.*.class though */
        Hdfs(strictSources, config)
        else if (args.boolean("hadoop1")) {
        config.set(CascadingFlowConnectorClassKey, DefaultHadoopFlowConnector)
        config.set(CascadingFlowProcessClassKey, DefaultHadoopFlowProcess)
        Hdfs(strictSources, config)
        } else if (args.boolean("hadoop2-mr1")) {
        config.set(CascadingFlowConnectorClassKey, DefaultHadoop2Mr1FlowConnector)
        config.set(CascadingFlowProcessClassKey, DefaultHadoop2Mr1FlowProcess)
        Hdfs(strictSources, config)
        } else if (args.boolean("hadoop2-tez")) {
        config.set(CascadingFlowConnectorClassKey, DefaultHadoop2TezFlowConnector)
        config.set(CascadingFlowProcessClassKey, DefaultHadoop2TezFlowProcess)
        Hdfs(strictSources, config)
        } else
        throw ArgsException(
            "[ERROR] Mode must be one of --local, --hadoop1, --hadoop2-mr1, --hadoop2-tez or --hdfs, you provided none"
        )
    }

  }

  implicit class JobStatsCompanionCascadingExtensions(jobstat: JobStats.type) {

    import cascading.stats.{CascadeStats, CascadingStats, FlowStats}

    def apply(stats: CascadingStats): JobStats = {
        val m: Map[String, Any] = statsMap(stats)
        new JobStats(stats match {
        case cs: CascadeStats => m
        case fs: FlowStats    => m + ("flow_step_stats" -> fs.getFlowStepStats.asScala.map(statsMap))
        })
    }

    private def counterMap(stats: CascadingStats): Map[String, Map[String, Long]] =
        stats.getCounterGroups.asScala.map { group =>
        (
            group,
            stats
            .getCountersFor(group)
            .asScala
            .map { counter =>
                (counter, stats.getCounterValue(group, counter))
            }
            .toMap
        )
        }.toMap

    private def statsMap(stats: CascadingStats): Map[String, Any] =
        Map(
        "counters" -> counterMap(stats),
        "duration" -> stats.getDuration,
        "finished_time" -> stats.getFinishedTime,
        "id" -> stats.getID,
        "name" -> stats.getName,
        "run_time" -> stats.getRunTime,
        "start_time" -> stats.getStartTime,
        "submit_time" -> stats.getSubmitTime,
        "failed" -> stats.isFailed,
        "skipped" -> stats.isSkipped,
        "stopped" -> stats.isStopped,
        "successful" -> stats.isSuccessful
        )

  }
}

object CascadingExtensions extends CascadingExtensions