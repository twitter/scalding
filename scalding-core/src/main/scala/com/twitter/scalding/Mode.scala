/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.scalding

import java.io._
import java.lang.reflect.Constructor
import java.nio.charset.Charset
import java.nio.file.{ Files, Paths }
import java.util
import java.util.{ Properties, UUID }

import cascading.flow.hadoop.{ HadoopFlowConnector, HadoopFlowProcess }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import cascading.flow._
import cascading.flow.local.LocalFlowConnector
import cascading.flow.local.LocalFlowProcess
import cascading.flow.planner.BaseFlowStep
import cascading.property.AppProps
import cascading.scheme.NullScheme
import cascading.tap.{ SinkMode, Tap }
import cascading.tuple.{ Fields, Tuple, TupleEntryIterator }
import com.twitter.maple.tap.MemorySourceTap
import com.twitter.scalding.StorageMode.TemporarySource
import com.twitter.scalding.filecache.{ CachedFile, UncachedFile }
import com.twitter.scalding.reducer_estimation.ReducerEstimatorStepStrategy
import com.twitter.scalding.typed.MemorySink

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.{ Set => MSet }
import scala.util.{ Failure, Success }
import org.slf4j.LoggerFactory

import scala.annotation.meta.param
import scala.collection.{ Map, mutable }

case class ModeException(message: String) extends RuntimeException(message)

case class ModeLoadException(message: String, origin: ClassNotFoundException) extends RuntimeException(origin)

object Mode {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
   * This is a Args and a Mode together. It is used purely as
   * a work-around for the fact that Job only accepts an Args object,
   * but needs a Mode inside.
   */
  private class ArgsWithMode(argsMap: scala.Predef.Map[String, List[String]], val mode: Mode) extends Args(argsMap) {
    override def +(keyvals: (String, Iterable[String])): Args =
      new ArgsWithMode(super.+(keyvals).m, mode)
  }

  /** Attach a mode to these Args and return the new Args */
  def putMode(mode: Mode, args: Args): Args = new ArgsWithMode(args.m, mode)

  /** Get a Mode if this Args was the result of a putMode */
  def getMode(args: Args): Option[Mode] = args match {
    case withMode: ArgsWithMode => Some(withMode.mode)
    case _ => None
  }

  val KnownModesMap = Seq(
    /* The modes are referenced by class *names* to avoid tightly coupling all the jars together. This class, Mode,
    must be loadable whether one of these is missing or not (it is unlikely all can be loaded at the same time)
     */
    "local" -> "com.twitter.scalding.Local",
    "hadoop2-mr1" -> "com.twitter.scalding.Hadoop2Mr1Mode",
    "hadoop2-tez" -> "com.twitter.scalding.TezMode", "tez" -> "com.twitter.scalding.TezMode",
    "hadoop1" -> "com.twitter.scalding.LegacyHadoopMode",
    "hdfs" -> "com.twitter.scalding.LegacyHadoopMode",
    "flink" -> "com.twitter.scalding.FlinkMode")

  val KnownTestModesMap = Seq(
    "local-test" -> "com.twitter.scalding.Test",
    "hadoop1-test" -> "com.twitter.scalding.HadoopTest",
    "hadoop2-mr1-test" -> "com.twitter.scalding.Hadoop2Mr1TestMode",
    "tez-test" -> "com.twitter.scalding.TezTestMode",
    "flink-test" -> "com.twitter.scalding.FlinkTestMode")
  // TODO: define hadoop2-mr1 (easy), tez and flink (less easy) classes.

  private def getModeConstructor[M <: Mode](clazzName: String, types: Class[_]*) =
    try {
      val clazz: Class[M] = Class.forName(clazzName).asInstanceOf[Class[M]]
      val ctor = clazz.getConstructor(types: _*)
      Some(ctor)
    } catch {
      case ncd: ClassNotFoundException => None
    }

  private def filterKnownModes[M <: Mode](clazzNames: Iterable[(String, String)], types: Class[_]*): Map[String, Constructor[M]] =
    clazzNames.map {
      case (modeName, clazzName) =>
        (modeName, getModeConstructor[M](clazzName, types: _*))
    }.collect { case (modeName, Some(ctor)) => (modeName, ctor) }.toMap

  private val RegularModeTypes = Seq(classOf[Boolean], classOf[Configuration])

  private lazy val ReallyAvailableModes = filterKnownModes[Mode](KnownModesMap, RegularModeTypes: _*)

  lazy private val KnownModesDict = KnownModesMap.toMap
  val KnownModeNames = KnownModesMap.map(_._1) // same order as KnownModesMap (priorities)

  private val TestModeTypes = Seq(classOf[Configuration], classOf[(Source) => Option[Buffer[Tuple]]])
  private lazy val ReallyAvailableTestModes = filterKnownModes[TestMode](KnownTestModesMap, TestModeTypes: _*)

  def test(modeName: String, config: Configuration, bufferMap: (Source) => Option[Buffer[Tuple]]): TestMode = {

    val modeCtor = KnownTestModesMap
      .filter { case (flag, _) => (flag == modeName) || ((modeName == "anyCluster-test") && (flag != "local-test")) }
      .map { case (flag, _) => ReallyAvailableTestModes.get(flag) }
      .collect { case Some(ctor) => ctor }
      .headOption

    construct[TestMode](modeCtor, config, bufferMap)
  }

  // This should be passed ALL the args supplied after the job name
  def apply(args: Args, config: Configuration): Mode = {
    val strictSources = args.boolean("tool.partialok") == false
    if (!strictSources) {
      LOG.info("using --tool.partialok. Missing log data won't cause errors.")
    }

    lazy val autoMode = if (args.boolean("autoCluster")) {
      KnownModeNames.toStream
        .filterNot(_ == "local")
        .map(ReallyAvailableModes.get).collect { case Some(ctor) => ctor }.headOption
    } else None

    val ctorFinder = (clazzName: String) => getModeConstructor[Mode](clazzName, RegularModeTypes: _*)

    lazy val requestedMode = KnownModeNames.find(args.boolean) // scanned in the order of KnownModesMap
    lazy val requestedModeCtor = requestedMode.flatMap(KnownModesDict.get).flatMap(ctorFinder)

    lazy val explicitModeClazzName = args.optional("modeClass") // use this to supply a custom Mode class from the command line
    lazy val explicitModeClazzCtor = explicitModeClazzName.flatMap(ctorFinder)

    val modeCtor = explicitModeClazzCtor.orElse(autoMode).orElse(requestedModeCtor)

    construct(modeCtor, boolean2Boolean(strictSources): AnyRef, config)
  }

  private def construct[M <: Mode](modeCtor: Option[Constructor[M]], args: AnyRef*): M = {
    modeCtor match {
      case Some(ctor) =>
        try {
          ctor.newInstance(args: _*)
        } catch {
          case ncd: ClassNotFoundException => {
            throw new ModeLoadException("Failed to load Scalding Mode class (missing runtime jar?) " + ctor, ncd)
          }
        }
      case None =>
        throw ArgsException("[ERROR] Mode must be one of --local, --hadoop1, --hadoop2-mr1, --hadoop2-tez or --hdfs, you provided none or none was found in the ClassPath")
    }
  }

}

trait ExecutionMode extends java.io.Serializable {
  /**
   * Using a new FlowProcess, which is only suitable for reading outside
   * of a map/reduce job, open a given tap and return the TupleEntryIterator
   */
  def openForRead(config: Config, tap: Tap[_, _, _]): TupleEntryIterator

  /** Create a new FlowConnector for this cascading planner */
  def newFlowConnector(props: Config): FlowConnector

  /**
   * Create a new Flow using the default FlowConnector, and ensure any fabric-specific flow configuration is complete.
   */
  def newFlow(config: Config, flowDef: FlowDef): Flow[_] = {
    val flowcon = newFlowConnector(config)
    val flow = flowcon.connect(flowDef)
    flow
  }

  /**
   * @return true if a checkpoint is required before a hash join on the given platform
   */
  private[scalding] def getHashJoinAutoForceRight: Boolean = false

  private[scalding] def defaultConfig: Config = Config.empty
}

trait StorageMode extends java.io.Serializable {
  import StorageMode._

  /**
   * @return true if the file exists on the current filesystem.
   */
  def fileExists(filename: String): Boolean

  def temporaryTypedSource[T]: TemporarySource[T]

  /**
   * Create a Tap for the purpose set out in {{readOrWrite}}, using the scheme provided by {{schemedSource}}
   */
  def createTap(schemedSource: SchemedSource, readOrWrite: AccessMode, mode: Mode, sinkMode: SinkMode): Tap[_, _, _]

  /**
   * Verify whether sources are valid, given the applicable source strictness mode
   *
   * @throws InvalidSourceException if sources are missing
   */
  def validateTap(schemedSource: SchemedSource): Unit

  /**
   * Create a memory-only tap for the purpose of spooling some tuples from memory.
   * On Local modes, it is also possible to write to a memory tap.
   */
  def createMemoryTap(readOrWrite: AccessMode, fields: Fields, asBuffer: mutable.Buffer[Tuple]): Tap[_, _, _]

  /** Create a write-only tap which simply discards the results */
  def createNullTap: Tap[_, _, _]

  // access this through the DistributedCacheFile object
  private[scalding] def addCachedFile(file: UncachedFile): CachedFile

  /** Read the content of a file into a string, UTF-8 encoding is assumed */
  def readFromFile(filename: String): String
  /** Read the content of a file into a string, UTF-8 encoding is assumed */
  def writeToFile(filename: String, text: String): Unit
}

object StorageMode {
  trait TemporarySource[T] {
    def sink(conf: Config): TypedSink[T]
    def downstreamPipe(conf: Config): TypedPipe[T]
  }
}

trait Mode extends java.io.Serializable {
  def storageMode: StorageMode
  def executionMode: ExecutionMode

  def strictSources: Boolean
  def name: String

  // Override this if you define new testing modes
  def isTesting: Boolean = false

  /**
   * Using a new FlowProcess, which is only suitable for reading outside
   * of a map/reduce job, open a given tap and return the TupleEntryIterator
   */
  final def openForRead(config: Config, tap: Tap[_, _, _]): TupleEntryIterator = executionMode.openForRead(config, tap)

  /** Create a new FlowConnector for this cascading planner */
  protected // will probably have to end up just "@deprecated"
  final def newFlowConnector(config: Config): FlowConnector = newFlowConnector(config)

  /** Create a new Flow for this cascading planner */
  def newFlow(config: Config, flowDef: FlowDef): Flow[_] = executionMode.newFlow(config, flowDef)

  /**
   * @return true if the file exists on the current filesystem.
   */
  final def fileExists(filename: String): Boolean = storageMode.fileExists(filename)

  @deprecated("A Config is needed, especially if any kryo serialization has been used", "0.12.0")
  final def openForRead(tap: Tap[_, _, _]): TupleEntryIterator =
    openForRead(Config.defaultFrom(this), tap)
}

/**
 * Any Mode that runs using local resources only
 */
trait LocalMode extends Mode {
}

/**
 * Any Mode that runs over a cluster (as opposed to Local) is a ClusterMode.
 */
trait ClusterMode extends Mode {
}

/**
 * Any ClusterMode whose main configuration is org.apache.hadoop.conf.Configuration is a HadoopFamilyMode
 */
trait HadoopFamilyMode extends ClusterMode {
  def jobConf: Configuration
}
/**
 * The "HadoopMode" is actually an alias for "a mode running on a fabric that ultimately runs using an execution
 * engine compatible with some of the Hadoop technology stack (may or may not include Hadoop 1.x, YARN, etc.)
 */
trait HadoopExecutionModeBase[ConfigType <: Configuration] extends ExecutionMode {
  def jobConf: Configuration
  def mode: Mode

  override def getHashJoinAutoForceRight: Boolean = {
    val config = Config.fromHadoop(jobConf)
    config.getHashJoinAutoForceRight
  }

  protected def enrichConfig(conf: Config): Map[AnyRef, AnyRef] = {
    val asMap = conf.toMap.toMap[AnyRef, AnyRef] // linter:ignore
    val jarKey = AppProps.APP_JAR_CLASS

    conf.getCascadingAppJar match {
      case Some(Success(cls)) => asMap + (jarKey -> cls)
      case Some(Failure(err)) =>
        // This may or may not cause the job to fail at submission, let's punt till then
        LoggerFactory.getLogger(getClass)
          .error(
            "Could not create class from: %s in config key: %s, Job may fail.".format(conf.get(jarKey), AppProps.APP_JAR_CLASS),
            err)
        // Just delete the key and see if it fails when cascading tries to submit
        asMap - jarKey
      case None => asMap
    }
  }

  protected def newFlowConnector(rawConf: java.util.Map[AnyRef, AnyRef]): FlowConnector
  override def newFlowConnector(conf: Config) = newFlowConnector(enrichConfig(conf).asJava)

  protected def newFlowProcess(conf: ConfigType): FlowProcess[ConfigType]

  protected def defaultConfiguration: ConfigType

  // TODO  unlike newFlowConnector, this does not look at the Job.config
  override def openForRead(config: Config, tap: Tap[_, _, _]) = {
    val htap = tap.asInstanceOf[Tap[ConfigType, _, _]]
    // copy over Config
    val conf = defaultConfiguration
    config.toMap.foreach { case (k, v) => conf.set(k, v) }

    val fp = newFlowProcess(conf)
    htap.retrieveSourceFields(fp)
    htap.sourceConfInit(fp, conf)
    htap.openForRead(fp)
  }

  override def defaultConfig: Config = Config.fromHadoop(jobConf) - Config.IoSerializationsKey

  override def newFlow(config: Config, flowDef: FlowDef): Flow[_] = {
    val flow = super.newFlow(config, flowDef)

    applyDescriptionsOnSteps(flow)
    applyReducerEstimationStrategies(flow, config)
    /* NOTE: it is not yet known whether the descriptions and the Reducer Estimation Strategies are *relevant* for
      non-MR engines (Tez, Flink).
      TODO: If they are, the actual way to convey these results *to* the fabric are not yet plugged in.
       */

    flow
  }

  private def getIdentifierOpt(descriptions: Seq[String]): Option[String] = {
    if (descriptions.nonEmpty) Some(descriptions.distinct.mkString(", ")) else None
  }

  private def updateStepConfigWithDescriptions(step: BaseFlowStep[JobConf]): Unit = {
    val conf = step.getConfig
    getIdentifierOpt(ExecutionContext.getDesc(step)).foreach(descriptionString => {
      conf.set(Config.StepDescriptions, descriptionString)
    })
  }

  def applyDescriptionsOnSteps(flow: Flow[_]): Unit = {
    val flowSteps = flow.getFlowSteps.asScala
    flowSteps.foreach {
      case baseFlowStep: BaseFlowStep[JobConf @unchecked] =>
        updateStepConfigWithDescriptions(baseFlowStep)
    }
  }

  def applyReducerEstimationStrategies(flow: Flow[_], config: Config): Unit = {

    // if any reducer estimators have been set, register the step strategy
    // which instantiates and runs them

    val reducerEstimatorStrategy: Seq[FlowStepStrategy[JobConf]] = config.get(Config.ReducerEstimators).toList.map(_ => ReducerEstimatorStepStrategy)

    val otherStrategies: Seq[FlowStepStrategy[JobConf]] = config.getFlowStepStrategies.map {
      case Success(fn) => fn(mode, config)
      case Failure(e) => throw new Exception("Failed to decode flow step strategy when submitting job", e)
    }

    val optionalFinalStrategy = FlowStepStrategies().sumOption(reducerEstimatorStrategy ++ otherStrategies)

    optionalFinalStrategy.foreach { strategy =>
      flow.setFlowStepStrategy(strategy)
    }

    config.getFlowListeners.foreach {
      case Success(fn) => flow.addListener(fn(mode, config))
      case Failure(e) => throw new Exception("Failed to decode flow listener", e)
    }

    config.getFlowStepListeners.foreach {
      case Success(fn) => flow.addStepListener(fn(mode, config))
      case Failure(e) => new Exception("Failed to decode flow step listener when submitting job", e)
    }
  }

}

class CascadingLocalExecutionMode extends ExecutionMode {
  override def newFlowConnector(conf: Config) =
    new LocalFlowConnector(conf.toMap.toMap[AnyRef, AnyRef].asJava) // linter:ignore

  override def openForRead(config: Config, tap: Tap[_, _, _]) = {
    val ltap = tap.asInstanceOf[Tap[Properties, _, _]]
    val props = new java.util.Properties
    config.toMap.foreach { case (k, v) => props.setProperty(k, v) }
    val fp = new LocalFlowProcess(props)
    ltap.retrieveSourceFields(fp)
    ltap.sourceConfInit(fp, props)
    ltap.openForRead(fp)
  }
}

trait TestStorageMode extends StorageMode {
  import StorageMode._

  private var fileSet = Set[String]()
  def registerTestFiles(files: Iterable[String]) = fileSet = files.toSet
  override def fileExists(filename: String): Boolean = fileSet.contains(filename)
}

trait LocalStorageModeCommon extends StorageMode {

  def createMemoryTap(createMemoryTap: AccessMode, fields: Fields, tupleBuffer: mutable.Buffer[Tuple]): Tap[_, _, _] =
    new MemoryTap[InputStream, OutputStream](new NullScheme(fields, fields), tupleBuffer)

  def createNullTap: Tap[_, _, _] = new NullTap[Properties, InputStream, OutputStream, Any, Any]

  def addCachedFile(file: UncachedFile): CachedFile = file.addLocal

  def readFromFile(filename: String): String =
    try {
      new String(Files.readAllBytes(Paths.get(filename)), Charset.forName("UTF-8"))
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }

  def writeToFile(filename: String, text: String): Unit =
    try {
      val br = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), Charset.forName("UTF-8")))
      br.write(text)
      br.close()
    } catch {
      case e: IOException => throw new RuntimeException(e)
    }

  def temporaryTypedSource[T]: TemporarySource[T] = new TemporarySource[T] {
    lazy val inMemoryDest = new MemorySink[T]

    override def sink(conf: Config): TypedSink[T] = inMemoryDest

    override def downstreamPipe(conf: Config): TypedPipe[T] = TypedPipe.from[T](inMemoryDest.readResults)
  }

}

class LocalStorageMode(strictSources: Boolean) extends TestStorageMode with LocalStorageModeCommon {
  override def fileExists(filename: String): Boolean = new File(filename).exists

  def createTap(schemedSource: SchemedSource, readOrWrite: AccessMode, mode: Mode, sinkMode: SinkMode): Tap[_, _, _] = {
    schemedSource match {
      case ltp: LocalTapProvider =>
        readOrWrite match {
          case Read => ltp.createLocalReadTap(sinkMode)
          case Write => ltp.createLocalWriteTap(sinkMode)
        }
      case _ => throw new ModeException("Cascading Local storage mode not supported for: " + schemedSource.toString)
    }
  }

  def validateTap(schemedSource: SchemedSource): Unit =
    schemedSource match {
      case ltp: LocalTapProvider => ltp.validateLocalTap(strictSources)
      case _ => throw new ModeException("Cascading Local storage mode not supported for: " + schemedSource.toString)
    }

}

trait HdfsStorageModeCommon extends StorageMode {
  def jobConf: Configuration

  def createMemoryTap(readOrWrite: AccessMode, fields: Fields, tupleBuffer: mutable.Buffer[Tuple]): Tap[_, _, _] =
    if (readOrWrite == Read)
      new MemorySourceTap(tupleBuffer.asJava, fields)
    else
      throw new UnsupportedOperationException(s"on non-Local storage mode, cannot build MemoryTap for ${readOrWrite} operation")

  def temporaryTypedSource[T]: TemporarySource[T] = new TemporarySource[T] {
    val cachedRandomUUID = java.util.UUID.randomUUID

    def hadoopTypedSource(conf: Config): TypedSource[T] with TypedSink[T] = {
      // come up with unique temporary filename, use the config here
      // TODO: refactor into TemporarySequenceFile class
      val tmpDir = conf.get("hadoop.tmp.dir")
        .orElse(conf.get("cascading.tmp.dir"))
        .getOrElse("/tmp")

      val tmpSeq = tmpDir + "/scalding/snapshot-" + cachedRandomUUID + ".seq"
      source.TypedSequenceFile[T](tmpSeq)
    }

    override def sink(conf: Config): TypedSink[T] = hadoopTypedSource(conf: Config)

    override def downstreamPipe(conf: Config): TypedPipe[T] = TypedPipe.from[T](hadoopTypedSource(conf))
  }

  def createNullTap: Tap[_, _, _] = new NullTap[JobConf, RecordReader[_, _], OutputCollector[_, _], Any, Any]

  def addCachedFile(file: UncachedFile): CachedFile = file.addHdfs(jobConf)

  def readFromFile(filename: String): String =
    try {
      val pt = new Path(filename)
      val fs = pt.getFileSystem(jobConf)
      fs.open(pt).readUTF
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }

  def writeToFile(filename: String, text: String): Unit =
    try {
      val pt = new Path(filename)
      val fs = pt.getFileSystem(jobConf)
      val br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)))

      br.write(text)
      br.close()
    } catch {
      case e: IOException => throw new RuntimeException(e)
    }

}

class HdfsTestStorageMode(strictSources: Boolean, @transient override val jobConf: Configuration, @transient pathAllocator: Source => String)
  extends StorageMode with TestStorageMode with HdfsStorageModeCommon {

  override def createTap(schemedSource: SchemedSource, readOrWrite: AccessMode,
    mode: Mode, sinkMode: SinkMode): Tap[_, _, _] =
    (mode, schemedSource) match {
      case (testMode: TestMode, tapProvider: HfsTapProvider) =>
        TestTapFactory(schemedSource, schemedSource.hdfsScheme, sinkMode).createHdfsTap(readOrWrite, testMode, pathAllocator, tapProvider)

      case (_: TestMode, _) =>
        throw new UnsupportedOperationException("HdfsTestStorageMode cannot create test tap: source doesn't provide Hfs taps")

      case _ =>
        throw new UnsupportedOperationException("HdfsTestStorageMode cannot create test tap in a non-testing mode")
    }

  def validateTap(schemedSource: SchemedSource): Unit = () // no path validation in test mode

}

class LocalTestStorageMode extends TestStorageMode with LocalStorageModeCommon {

  override def createTap(schemedSource: SchemedSource, readOrWrite: AccessMode, mode: Mode, sinkMode: SinkMode): Tap[_, _, _] =
    (mode, readOrWrite) match {
      case (testMode: TestMode, Read) =>
        TestTapFactory(schemedSource, schemedSource.localScheme.getSourceFields, sinkMode).createLocalTap(readOrWrite, testMode)
      case (testMode: TestMode, Write) =>
        TestTapFactory(schemedSource, schemedSource.localScheme.getSinkFields, sinkMode).createLocalTap(readOrWrite, testMode)
      case _ =>
        throw new UnsupportedOperationException("LocalTestStorageMode cannot create test tap in a non-testing mode")
    }

  def validateTap(schemedSource: SchemedSource): Unit = () // no path validation in test mode
}

/**
 * Any
 */
class HdfsStorageMode(strictSources: Boolean, @transient override val jobConf: Configuration) extends StorageMode with HdfsStorageModeCommon {
  import StorageMode._
  override def fileExists(filename: String): Boolean = {
    val path = new Path(filename)
    path.getFileSystem(jobConf).exists(path)
  }

  def createTap(schemedSource: SchemedSource, readOrWrite: AccessMode, mode: Mode, sinkMode: SinkMode): Tap[_, _, _] = {
    schemedSource match {
      case htp: HfsTapProvider =>
        readOrWrite match {
          case Read => htp.createHdfsReadTap(strictSources, jobConf, mode, sinkMode)
          case Write => CastHfsTap(htp.createHdfsWriteTap(sinkMode))
        }
      case _ => throw new ModeException("Cascading HDFS storage mode not supported for: " + schemedSource.toString)
    }
  }

  def validateTap(schemedSource: SchemedSource): Unit =
    schemedSource match {
      case htp: HfsTapProvider => htp.validateHdfsTap(strictSources, jobConf)
      case _ => throw new ModeException("Cascading HDFS storage mode not supported for: " + schemedSource.toString)
    }
}

case class Local(strictSources: Boolean) extends LocalMode {

  // this auxiliary ctor is provided for compatibility with the dynamic load made by Mode.apply
  def this(strictSources: Boolean, @(transient @param) dummyJobConf: Configuration) = this(strictSources)

  val name = "local"

  override val storageMode: StorageMode = new LocalStorageMode(strictSources)
  override val executionMode: ExecutionMode = new CascadingLocalExecutionMode
}

//object Local {
//  def apply(strictSources: Boolean): Local = new Local(strictSources, new Configuration)
//}

/**
 * Memory only testing for unit tests
 */

trait TestMode extends Mode {
  override def storageMode: TestStorageMode
  override def isTesting = true
  def buffers: Source => Option[Buffer[Tuple]]

  def registerTestFiles(files: Iterable[String]): Unit = storageMode.registerTestFiles(files)

  /**
   * Perform any closing activity required on a set of sinks, if any
   *
   * Note: this is typically used for test purposes.
   *
   * @param sinks a list of sinks to visit and on which to perform post-job activity
   */
  def finalize(sinks: Iterable[Source]): Unit = ()
}

case class Test(override val buffers: (Source) => Option[Buffer[Tuple]]) extends LocalMode with TestMode {

  /* this is the ctor that is found via reflection by @{link Mode.test} */
  def this(unused: Configuration, buffers: (Source) => Option[Buffer[Tuple]]) = {
    this(buffers)
  }

  val strictSources = false
  val name = "local-test"

  override val storageMode: TestStorageMode = new LocalTestStorageMode()
  override val executionMode: ExecutionMode = new CascadingLocalExecutionMode
}

trait HadoopFamilyTestMode extends HadoopFamilyMode with TestMode {
  // This is a map from source.toString to disk path
  private val writePaths = MMap[Source, String]()
  private val allPaths = MSet[String]()

  @tailrec
  private def allocateNewPath(prefix: String, idx: Int): String = {
    val candidate = prefix + idx.toString
    if (allPaths(candidate)) {
      //Already taken, try again:
      allocateNewPath(prefix, idx + 1)
    } else {
      // Update all paths:
      allPaths += candidate
      candidate
    }
  }

  private val thisTestID = UUID.randomUUID
  private lazy val basePath = {
    val r = s"/tmp/scalding/${name}-${thisTestID}/"
    new File(r).mkdirs()
    r
  }

  /** Looks up a local path to write the given source to */
  def getWritePathFor(src: Source): String = {
    val rndIdx = new java.util.Random().nextInt(1 << 30)
    writePaths.getOrElseUpdate(src, allocateNewPath(basePath +
      src.toString
      .replaceAll("[/\\\\\\$ .\\(\\),;:\t\\[\\]\\+'\"]", "_")
      .replaceAll("_+", "_") + "_", rndIdx))
  }

  private def finalizeSink(src: Source): Unit = {
    /* The following `_.get` is only safe if `src` belongs to the source map.
     * This invariant is preserved by the `JobTest.sink` and `JobTest.runJob`
     * functions, and those functions have been documented accordingly to
     * warn about this invariant.
     */
    @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.OptionPartial")) // Get the buffer for the given source, and empty it:
    val buf = buffers(src).get
    buf.clear()
    // Now fill up this buffer with the content of the file
    val path = getWritePathFor(src)
    // We read the write tap in order to add its contents in the test buffers
    val it = openForRead(Config.defaultFrom(this), src.createTap(Write)(this))
    while (it != null && it.hasNext) {
      buf += new Tuple(it.next.getTuple)
    }
    it.close()
    //Clean up this data off the disk
    new File(path).delete()
    writePaths -= src
  }

  override def finalize(sinks: Iterable[Source]): Unit = {
    sinks.foreach(finalizeSink)
    // since all files finalized already, this should be empty now.
    new File(basePath).delete
  }

}