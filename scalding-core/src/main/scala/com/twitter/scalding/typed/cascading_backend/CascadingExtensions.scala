package com.twitter.scalding.typed.cascading_backend

import cascading.flow.{Flow, FlowDef, FlowConnector}
import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.{Fields, TupleEntryIterator}
import com.twitter.scalding.cascading_interop.FlowListenerPromise
import com.twitter.scalding.filecache.{CachedFile, DistributedCacheFile}
import com.twitter.scalding.mathematics.{MatrixLiteral, Matrix2}
import com.twitter.scalding.typed.KeyedListLike
import org.apache.hadoop.conf.Configuration
import scala.concurrent.{Future, ExecutionContext => ConcurrentExecutionContext}
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

  abstract class TypedPipeLikeExtensions[A, T <: A] {
    def toTypedPipe: TypedPipe[T]
    /**
     * Export back to a raw cascading Pipe. useful for interop with the scalding Fields API or with Cascading
     * code. Avoid this if possible. Prefer to write to TypedSink.
     */
    final def toPipe[U >: T](
        fieldNames: Fields
    )(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe =
        // we have to be cafeful to pass the setter we want since a low priority implicit can always be
        // found :(
        CascadingBackend.toPipe[U](toTypedPipe.withLine, fieldNames)(flowDef, mode, setter)

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
            case ivs: InvalidSourceException => toTypedPipe.writeThrough(dest)
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
        val next = toTypedPipe.withLine
        FlowStateMap.merge(flowDef, FlowState.withTypedWrite(next, dest, mode))
        next
    }
  }

  implicit class TypedPipeCascadingExtensions[T](val toTypedPipe: TypedPipe[T]) extends TypedPipeLikeExtensions[Any, T]

  implicit class KeyedListCascadingExtensions[K, V, S[K, +V] <: KeyedListLike[K, V, S]](
    keyed: KeyedListLike[K, V, S]) extends TypedPipeLikeExtensions[(K, V), (K, V)] {
    def toTypedPipe = keyed.toTypedPipe
  }

  implicit class ValuePipeCascadingExtensions[T](value: ValuePipe[T]) extends TypedPipeLikeExtensions[Any, T] {
    def toTypedPipe = value.toTypedPipe
  }

  implicit class MappableCascadingExtensions[T](mappable: Mappable[T]) extends TypedPipeLikeExtensions[Any, T] {
    def toTypedPipe = TypedPipe.from(mappable)
  }

  implicit class ExecutionCompanionCascadingExtensions(ex: Execution.type) {
    def fromFn(fn: (Config, Mode) => FlowDef): Execution[Unit] =
      Execution.backendSpecific(CascadingExtensions.FromFnToBackend(fn))

    /**
     * Distributes the file onto each map/reduce node, so you can use it for Scalding source creation and
     * TypedPipe, KeyedList, etc. transformations. Using the [[com.twitter.scalding.filecache.CachedFile]]
     * outside of Execution will probably not work.
     *
     * For multiple files you must nested your execution, see docs of
     * [[com.twitter.scalding.filecache.DistributedCacheFile]]
     */
     def withCachedFile[T](path: String)(fn: CachedFile => Execution[T]): Execution[T] =
       DistributedCacheFile.execution(path)(fn)

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

  implicit class ConfigCascadingExtensions(config: Config) {
    import cascading.flow.{FlowListener, FlowProps, FlowStepListener, FlowStepStrategy}
    import com.twitter.bijection.{Base64String, Injection}
    import com.twitter.chill.{Externalizer, ExternalizerCodec, ExternalizerInjection, KryoInstantiator}
    import com.twitter.chill.config.{ConfiguredInstantiator, ScalaMapConfig}
    import com.twitter.scalding.filecache.{CachedFile, DistributedCacheFile}
    import org.apache.hadoop.mapred.JobConf
    import org.apache.hadoop.io.serializer.{Serialization => HSerialization}
    /**
     * Add files to be localized to the config. Intended to be used by user code.
     * @param cachedFiles
     *   CachedFiles to be added
     * @return
     *   new Config with cached files
     */
    def addDistributedCacheFiles(cachedFiles: CachedFile*): Config =
      DistributedCacheFile.addDistributedCacheFiles(config, cachedFiles: _*)

    /**
     * Get cached files from config
     */
    def getDistributedCachedFiles: Seq[CachedFile] =
      DistributedCacheFile.getDistributedCachedFiles(config)

    def getCascadingSerializationTokens: Map[Int, String] =
      config.get(Config.CascadingSerializationTokens)
        .map(CascadingTokenUpdater.parseTokens)
        .getOrElse(Map.empty[Int, String])

    /*
    * If a ConfiguredInstantiator has been set up, this returns it
    */
    def getKryo: Option[KryoInstantiator] =
      if (config.toMap.contains(ConfiguredInstantiator.KEY))
        Some((new ConfiguredInstantiator(ScalaMapConfig(config.toMap))).getDelegate)
      else None

    /**
     * This function gets the set of classes that have been registered to Kryo. They may or may not be used in
     * this job, but Cascading might want to be made aware that these classes exist
     */
    def getKryoRegisteredClasses: Set[Class[_]] =
      // Get an instance of the Kryo serializer (which is populated with registrations)
      config.getKryo
        .map { kryo =>
          val cr = kryo.newKryo.getClassResolver

          @annotation.tailrec
          def kryoClasses(idx: Int, acc: Set[Class[_]]): Set[Class[_]] =
            Option(cr.getRegistration(idx)) match {
              case Some(reg) => kryoClasses(idx + 1, acc + reg.getType)
              case None      => acc // The first null is the end of the line
            }

          kryoClasses(0, Set[Class[_]]())
        }
        .getOrElse(Set())

    /*
    * Hadoop and Cascading serialization needs to be first, and the Kryo serialization
    * needs to be last and this method handles this for you:
    * hadoop, cascading, [userHadoop,] kyro
    * is the order.
    *
    * Kryo uses the ConfiguredInstantiator, which is configured either by reflection:
    * Right(classOf[MyInstantiator]) or by serializing given Instantiator instance
    * with a class to serialize to bootstrap the process:
    * Left((classOf[serialization.KryoHadoop], myInstance))
    */
    def setSerialization(
        kryo: Either[(Class[_ <: KryoInstantiator], KryoInstantiator), Class[_ <: KryoInstantiator]],
        userHadoop: Seq[Class[_ <: HSerialization[_]]] = Nil
    ): Config = {

      // Hadoop and Cascading should come first
      val first: Seq[Class[_ <: HSerialization[_]]] =
        Seq(
          classOf[org.apache.hadoop.io.serializer.WritableSerialization],
          classOf[cascading.tuple.hadoop.TupleSerialization],
          classOf[serialization.WrappedSerialization[_]]
        )
      // this must come last
      val last: Seq[Class[_ <: HSerialization[_]]] = Seq(classOf[com.twitter.chill.hadoop.KryoSerialization])
      val required = (first ++ last).toSet[AnyRef] // Class is invariant, but we use it as a function
      // Make sure we keep the order correct and don't add the required fields twice
      val hadoopSer = first ++ (userHadoop.filterNot(required)) ++ last

      val hadoopKV = Config.IoSerializationsKey -> hadoopSer.map(_.getName).mkString(",")

      // Now handle the Kryo portion which uses another mechanism
      val chillConf = ScalaMapConfig(config.toMap)
      kryo match {
        case Left((bootstrap, inst)) => ConfiguredInstantiator.setSerialized(chillConf, bootstrap, inst)
        case Right(refl)             => ConfiguredInstantiator.setReflect(chillConf, refl)
      }
      val withKryo = Config(chillConf.toMap + hadoopKV)

      val kryoClasses = withKryo.getKryoRegisteredClasses
        .filterNot(_.isPrimitive) // Cascading handles primitives and arrays
        .filterNot(_.isArray)

      withKryo.addCascadingClassSerializationTokens(kryoClasses)
    }

    def setDefaultComparator(clazz: Class[_ <: java.util.Comparator[_]]): Config =
      config + (FlowProps.DEFAULT_ELEMENT_COMPARATOR -> clazz.getName)

    /**
     * The serialization of your data will be smaller if any classes passed between tasks in your job are listed
     * here. Without this, strings are used to write the types IN EACH RECORD, which compression probably takes
     * care of, but compression acts AFTER the data is serialized into buffers and spilling has been triggered.
     */
    def addCascadingClassSerializationTokens(clazzes: Set[Class[_]]): Config =
      CascadingTokenUpdater.update(config, clazzes)

    def getSubmittedTimestamp: Option[RichDate] =
      config.get(Config.ScaldingFlowSubmittedTimestamp).map(ts => RichDate(ts.toLong))
    /*
    * Sets the timestamp only if it was not already set. This is here
    * to prevent overwriting the submission time if it was set by an
    * previously (or externally)
    */
    def maybeSetSubmittedTimestamp(date: RichDate = RichDate.now): (Option[RichDate], Config) =
      config.update(Config.ScaldingFlowSubmittedTimestamp) {
        case s @ Some(ts) => (s, Some(RichDate(ts.toLong)))
        case None         => (Some(date.timestamp.toString), None)
      }
    /**
     * configure flow listeneres for observability
     */
    def addFlowListener(flowListenerProvider: (Mode, Config) => FlowListener): Config = {
      val serializedListener = flowListenerSerializer(flowListenerProvider)
      config.update(Config.FlowListeners) {
        case None      => (Some(serializedListener), ())
        case Some(lst) => (Some(s"$serializedListener,$lst"), ())
      }._2
    }

    def getFlowListeners: List[Try[(Mode, Config) => FlowListener]] =
      config.get(Config.FlowListeners).toList
        .flatMap(s => StringUtility.fastSplit(s, ","))
        .map(flowListenerSerializer.invert(_))

    def addFlowStepListener(flowListenerProvider: (Mode, Config) => FlowStepListener): Config = {
      val serializedListener = flowStepListenerSerializer(flowListenerProvider)
      config.update(Config.FlowStepListeners) {
        case None      => (Some(serializedListener), ())
        case Some(lst) => (Some(s"$serializedListener,$lst"), ())
      }._2
    }

    def getFlowStepListeners: List[Try[(Mode, Config) => FlowStepListener]] =
      config.get(Config.FlowStepListeners).toList
        .flatMap(s => StringUtility.fastSplit(s, ","))
        .map(flowStepListenerSerializer.invert(_))

    def addFlowStepStrategy(flowStrategyProvider: (Mode, Config) => FlowStepStrategy[JobConf]): Config = {
      val serializedListener = flowStepStrategiesSerializer(flowStrategyProvider)
      config.update(Config.FlowStepStrategies) {
        case None      => (Some(serializedListener), ())
        case Some(lst) => (Some(s"$serializedListener,$lst"), ())
      }._2
    }

    def clearFlowStepStrategies: Config =
      config.-(Config.FlowStepStrategies)

    def getFlowStepStrategies: List[Try[(Mode, Config) => FlowStepStrategy[JobConf]]] =
      config.get(Config.FlowStepStrategies).toList
        .flatMap(s => StringUtility.fastSplit(s, ","))
        .map(flowStepStrategiesSerializer.invert(_))

    private[this] def buildInj[T: ExternalizerInjection: ExternalizerCodec]: Injection[T, String] =
      Injection.connect[T, Externalizer[T], Array[Byte], Base64String, String]

    private[scalding] def flowStepListenerSerializer =
      buildInj[(Mode, Config) => FlowStepListener]
    private[scalding] def flowListenerSerializer = buildInj[(Mode, Config) => FlowListener]
    private[scalding] def flowStepStrategiesSerializer =
      buildInj[(Mode, Config) => FlowStepStrategy[JobConf]]
    private[scalding] def argsSerializer = buildInj[Map[String, List[String]]]
  }

  implicit class ConfigCompanionCascadingExtensions(config: Config.type) {
    import org.apache.hadoop.conf.Configuration
    /*
    * Note that Hadoop Configuration is mutable, but Config is not. So a COPY is
    * made on calling here. If you need to update Config, you do it by modifying it.
    * This copy also forces all expressions in values to be evaluated, freezing them
    * as well.
    */
    def fromHadoop(conf: Configuration): Config =
      // use `conf.get` to force JobConf to evaluate expressions
      Config(conf.asScala.map(e => e.getKey -> conf.get(e.getKey)).toMap)

    /*
    * For everything BUT SERIALIZATION, this prefers values in conf,
    * but serialization is generally required to be set up with Kryo
    * (or some other system that handles general instances at runtime).
    */
    def hadoopWithDefaults(conf: Configuration): Config =
      ((Config.default ++ fromHadoop(conf))
        .setSerialization(Right(classOf[serialization.KryoHadoop]))
        .setScaldingVersion
        .setHRavenHistoryUserName)
  }

  implicit class UniqueIDCompanionCascadingExtensions(uid: UniqueID.type) {
    def getIDFor(implicit fd: FlowDef): UniqueID =
      /*
      * In real deploys, this can even be a constant, but for testing
      * we need to allocate unique IDs to prevent different jobs running
      * at the same time from touching each other's counters.
      */
      UniqueID.fromSystemHashCode(fd)
  }

  implicit class Matrix2Extensions[R, C, V](mat: Matrix2[R, C, V]) {
    def write(sink: TypedSink[(R, C, V)])(implicit fd: FlowDef, m: Mode): Matrix2[R, C, V] = {
      import mat.{rowOrd, colOrd}
      MatrixLiteral(mat.toTypedPipe.write(sink), mat.sizeHint)
    }
  }
}

object CascadingExtensions extends CascadingExtensions {
  // This case class preserves equality on Executions
  private case class FromFnToBackend(fn: (Config, Mode) => FlowDef) extends
    Function4[Config, Mode, Execution.Writer, ConcurrentExecutionContext, CFuture[(Long, ExecutionCounters, Unit)]] {
    def apply(conf: Config, mode: Mode, writer: Execution.Writer, ec: ConcurrentExecutionContext) =
      writer match {
        case afdr: AsyncFlowDefRunner =>
          afdr
            .validateAndRun(conf)(fn(_, mode))(ec)
            .map { case (id, cnt) => (id, cnt, ()) }(ec)
        case _ =>
          CFuture.failed(
            new IllegalArgumentException(
              s"Execution.fromFn requires cascading Mode producing AsyncFlowDefRunner, found mode: $mode and writer ${writer.getClass}: $writer"
            )
          )
      }
  }
}