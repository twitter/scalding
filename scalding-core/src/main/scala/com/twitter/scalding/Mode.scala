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

import java.io.File
import java.util.{ UUID, Properties }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapred.JobConf

import cascading.flow.{ FlowProcess, FlowConnector, FlowDef, Flow }
import cascading.flow.local.LocalFlowConnector
import cascading.flow.local.LocalFlowProcess
import cascading.property.AppProps
import cascading.tap.Tap
import cascading.tuple.Tuple
import cascading.tuple.TupleEntryIterator

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.{ Set => MSet }
import scala.util.{ Failure, Success }

import org.slf4j.LoggerFactory

case class ModeException(message: String) extends RuntimeException(message)

case class ModeLoadException(message: String, origin: ClassNotFoundException) extends RuntimeException(origin)

object Mode {
  /**
   * This is a Args and a Mode together. It is used purely as
   * a work-around for the fact that Job only accepts an Args object,
   * but needs a Mode inside.
   */
  private class ArgsWithMode(argsMap: Map[String, List[String]], val mode: Mode) extends Args(argsMap) {
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

  val CascadingFlowConnectorClassKey = "cascading.flow.connector.class"
  val CascadingFlowProcessClassKey = "cascading.flow.process.class"
  val CascadingFlowProcessConfigClassKey = "cascading.flow.process.config.class"

  case class FabricSelector(flowConnectorClassName: String, flowProcessClassName: String, flowProcessConfigClassName: String) {
    def exists = {
      try {
        val k1 = Class.forName(flowConnectorClassName)
        val k2 = Class.forName(flowProcessClassName)
        val k3 = Class.forName(flowProcessConfigClassName)
        (k1 != null) && (k2 != null) && (k3 != null)
      } catch {
        case cnfe: ClassNotFoundException => false
      }
    }

    def selectInto(config: Configuration) = {
      config.set(CascadingFlowConnectorClassKey, flowConnectorClassName)
      config.set(CascadingFlowProcessClassKey, flowProcessClassName)
      config.set(CascadingFlowProcessConfigClassKey, flowProcessConfigClassName)
    }
  }

  val DefaultHadoopFlowConnector = "cascading.flow.hadoop.HadoopFlowConnector"
  val DefaultHadoopFlowProcess = "cascading.flow.hadoop.HadoopFlowProcess"
  val DefaultHadoopFlowProcessConfig = "org.apache.hadoop.mapred.JobConf"
  val DefaultHadoopFabric = FabricSelector(DefaultHadoopFlowConnector, DefaultHadoopFlowProcess, DefaultHadoopFlowProcessConfig)

  val DefaultHadoop2Mr1FlowConnector = "cascading.flow.hadoop2.Hadoop2MR1FlowConnector"
  val DefaultHadoop2Mr1FlowProcess = DefaultHadoopFlowProcess // no Hadoop2MR1FlowProcess as of Cascading 3.0.0-wip-75?
  val DefaultHadoop2Mr1FlowProcessConfig = DefaultHadoopFlowProcessConfig
  val DefaultHadoop2Mr1Fabric = FabricSelector(DefaultHadoop2Mr1FlowConnector, DefaultHadoop2Mr1FlowProcess, DefaultHadoop2Mr1FlowProcessConfig)

  val DefaultHadoop2TezFlowConnector = "cascading.flow.tez.Hadoop2TezFlowConnector"
  val DefaultHadoop2TezFlowProcess = "cascading.flow.tez.Hadoop2TezFlowProcess"
  val DefaultHadoop2TezFlowProcessConfig = "org.apache.tez.dag.api.TezConfiguration"
  val DefaultHadoop2TezFabric = FabricSelector(DefaultHadoop2TezFlowConnector, DefaultHadoop2TezFlowProcess, DefaultHadoop2TezFlowProcessConfig)

  val DefaultFlinkFlowConnector = "com.dataartisans.flink.cascading.FlinkConnector"
  val DefaultFlinkFlowProcess = "com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess"
  val DefaultFlinkFlowProcessConfiguration = "org.apache.hadoop.conf.Configuration"
  val DefaultFlinkFabric = FabricSelector(DefaultFlinkFlowConnector, DefaultFlinkFlowProcess, DefaultFlinkFlowProcessConfiguration)

  private lazy val selectedFabric = {
    val candidates = Seq(DefaultHadoop2TezFabric, DefaultHadoopFabric, DefaultHadoop2Mr1Fabric, DefaultFlinkFabric)

    val selected = candidates.find(_.exists)
    if (selected.isEmpty) {
      throw new IllegalArgumentException("Can't find a default Cascading fabric. Have you put one in the CLASSPATH?")
    }

    LoggerFactory.getLogger(getClass)
      .info(s"Using Cascading Flow Connector: ${selected.get.flowConnectorClassName} found in CLASSPATH")
    selected.get
  }

  def setDefaultFabricFromClasspath(config: Configuration) = {
    selectedFabric.selectInto(config)
  }

  // This should be passed ALL the args supplied after the job name
  def apply(args: Args, config: Configuration): Mode = {
    val strictSources = args.boolean("tool.partialok") == false
    if (!strictSources) {
      // TODO we should do smarter logging here
      println("[Scalding:INFO] using --tool.partialok. Missing log data won't cause errors.")
    }

    if (args.boolean("local"))
      Local(strictSources)
    else if (args.boolean("hdfs")) {
      setDefaultFabricFromClasspath(config)
      Hdfs(strictSources, config)
    } else if (args.boolean("hadoop1")) {
      DefaultHadoopFabric.selectInto(config)
      Hdfs(strictSources, config)
    } else if (args.boolean("hadoop2-mr1")) {
      DefaultHadoop2Mr1Fabric.selectInto(config)
      Hdfs(strictSources, config)
    } else if (args.boolean("hadoop2-tez")) {
      DefaultHadoop2TezFabric.selectInto(config)
      Hdfs(strictSources, config)
    } else if (args.boolean("flink")) {
      DefaultFlinkFabric.selectInto(config)
      Hdfs(strictSources, config)
    } else
      throw ArgsException("[ERROR] Mode must be one of --local, --hadoop1, --hadoop2-mr1, --hadoop2-tez, --flink or --hdfs, you provided none")
  }
}

trait Mode extends java.io.Serializable {
  /*
   * Using a new FlowProcess, which is only suitable for reading outside
   * of a map/reduce job, open a given tap and return the TupleEntryIterator
   */
  def openForRead(config: Config, tap: Tap[_, _, _]): TupleEntryIterator

  @deprecated("A Config is needed, especially if any kryo serialization has been used", "0.12.0")
  final def openForRead(tap: Tap[_, _, _]): TupleEntryIterator =
    openForRead(Config.defaultFrom(this), tap)

  // Returns true if the file exists on the current filesystem.
  def fileExists(filename: String): Boolean
  /** Create a new FlowConnector for this cascading planner */
  def newFlowConnector(props: Config): FlowConnector
}

trait HadoopMode extends Mode {
  def jobConf: Configuration

  override def newFlowConnector(conf: Config) = {
    val asMap = conf.toMap.toMap[AnyRef, AnyRef]
    val jarKey = AppProps.APP_JAR_CLASS

    val finalMap = conf.getCascadingAppJar match {
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

    val flowConnectorClass = jobConf.get(Mode.CascadingFlowConnectorClassKey, Mode.DefaultHadoopFlowConnector)

    try {
      val clazz = Class.forName(flowConnectorClass)
      val ctor = clazz.getConstructor(classOf[java.util.Map[_, _]])
      ctor.newInstance(finalMap.asJava).asInstanceOf[FlowConnector]
    } catch {
      case ncd: ClassNotFoundException => {
        throw new ModeLoadException("Failed to load Cascading flow connector class " + flowConnectorClass, ncd)
      }
    }
  }

  // TODO  unlike newFlowConnector, this does not look at the Job.config
  override def openForRead(config: Config, tap: Tap[_, _, _]) = {
    val htap: Tap[Configuration, _, _] = tap.asInstanceOf[Tap[Configuration, _, _]]

    val flowProcessClass = jobConf.get(Mode.CascadingFlowProcessClassKey, Mode.DefaultHadoopFlowProcess)
    val flowProcessConfigClass = jobConf.get(Mode.CascadingFlowProcessConfigClassKey, Mode.DefaultHadoopFlowProcessConfig)

    val (fp, conf) = try {
      val clazz = Class.forName(flowProcessClass)
      val confClazz = Class.forName(flowProcessConfigClass)
      if (!classOf[Configuration].isAssignableFrom(confClazz)) {
        throw new IllegalArgumentException(s"FlowProcess configuration type ${confClazz} does not implement ${classOf[Configuration]}")
      }


      val conf = {
        try {
          /* first constructor attempted: supposed to accept a Boolean where "true" means "load system defaults" */
          val confCtor = confClazz.getConstructor(java.lang.Boolean.TYPE)
          confCtor.newInstance(java.lang.Boolean.TRUE).asInstanceOf[Configuration]
        } catch {
          case _ : NoSuchMethodError | _ : NoSuchMethodException => {
            /* fallback: the Configuration should have a default constructor */
            val confCtor = confClazz.getConstructor()
            confCtor.newInstance().asInstanceOf[Configuration]
          }
        }
      } // initialize the default config
      // copy over Config
      config.toMap.foreach{ case (k, v) => conf.set(k, v) }


      val ctor = clazz.getConstructor(confClazz)
      val inst = ctor.newInstance(conf)
      (inst.asInstanceOf[FlowProcess[_ <: Configuration]], conf)
    } catch {
      case ncd: ClassNotFoundException => {
        throw new ModeLoadException("Failed to load Cascading flow process class " + flowProcessClass, ncd)
      }
    }

    htap.retrieveSourceFields(fp)
    htap.sourceConfInit(fp, conf)
    htap.openForRead(fp)
  }
}

trait CascadingLocal extends Mode {
  override def newFlowConnector(conf: Config) =
    new LocalFlowConnector(conf.toMap.toMap[AnyRef, AnyRef].asJava)

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

// Mix-in trait for test modes; overrides fileExists to allow the registration
// of mock filenames for testing.
trait TestMode extends Mode {
  private var fileSet = Set[String]()
  def registerTestFiles(files: Set[String]) = fileSet = files
  override def fileExists(filename: String): Boolean = fileSet.contains(filename)
}

case class Hdfs(strict: Boolean, @transient conf: Configuration) extends HadoopMode {
  override def jobConf = conf
  override def fileExists(filename: String): Boolean =
    FileSystem.get(jobConf).exists(new Path(filename))
}

case class HadoopTest(@transient conf: Configuration,
  @transient buffers: Source => Option[Buffer[Tuple]])
  extends HadoopMode with TestMode {

  // This is a map from source.toString to disk path
  private val writePaths = MMap[Source, String]()
  private val allPaths = MSet[String]()

  override def jobConf = conf

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
  private val basePath = "/tmp/scalding/%s/".format(thisTestID)
  // Looks up a local path to write the given source to
  def getWritePathFor(src: Source): String = {
    val rndIdx = new java.util.Random().nextInt(1 << 30)
    writePaths.getOrElseUpdate(src, allocateNewPath(basePath + src.getClass.getName, rndIdx))
  }

  def finalize(src: Source) {
    // Get the buffer for the given source, and empty it:
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
}

case class Local(strictSources: Boolean) extends CascadingLocal {
  override def fileExists(filename: String): Boolean = new File(filename).exists
}

/**
 * Memory only testing for unit tests
 */
case class Test(buffers: (Source) => Option[Buffer[Tuple]]) extends TestMode with CascadingLocal
