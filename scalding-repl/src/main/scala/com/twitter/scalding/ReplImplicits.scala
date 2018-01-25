/*  Copyright 2013 Twitter, inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FsShell, FileSystem }
import typed.KeyedListLike
import scala.util.{ Failure, Success }
import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext }

/**
 * Object containing various implicit conversions required to create Scalding flows in the REPL.
 * Most of these conversions come from the [[com.twitter.scalding.Job]] class.
 */
trait BaseReplState {

  /** required for switching to hdfs local mode */
  private val mr1Key = "mapred.job.tracker"
  private val mr2Key = "mapreduce.framework.name"
  private val mrLocal = "local"

  /** Implicit flowDef for this Scalding shell session. */
  var flowDef: FlowDef = getEmptyFlowDef
  /** Defaults to running in local mode if no mode is specified. */
  var mode: Mode = com.twitter.scalding.Local(false)
  /**
   * If the repl is started in Hdfs mode, this field is used to preserve the settings
   * when switching Modes.
   */
  var storedHdfsMode: Option[Hdfs] = None

  /** Switch to Local mode */
  def useLocalMode(): Unit = {
    mode = Local(false)
    printModeBanner()
  }

  def useStrictLocalMode(): Unit = {
    mode = Local(true)
    printModeBanner()
  }

  /** Switch to Hdfs mode */
  private def useHdfsMode_(): Unit = {
    storedHdfsMode match {
      case Some(hdfsMode) => mode = hdfsMode
      case None => println("To use HDFS/Hadoop mode, you must *start* the repl in hadoop mode to get the hadoop configuration from the hadoop command.")
    }
  }

  def useHdfsMode(): Unit = {
    useHdfsMode_()
    customConfig -= mr1Key
    customConfig -= mr2Key
    printModeBanner()
  }

  def useHdfsLocalMode(): Unit = {
    useHdfsMode_()
    customConfig += mr1Key -> mrLocal
    customConfig += mr2Key -> mrLocal
    printModeBanner()
  }

  private[scalding] def printModeBanner(): Unit = {
    val (modeString, homeDir) = mode match {
      case localMode: Local => {
        (localMode.toString, System.getProperty("user.dir"))
      }
      case hdfsMode: Hdfs => {
        val defaultFs = FileSystem.get(hdfsMode.jobConf)
        val m = customConfig.get(mr2Key) match {
          case Some("local") =>
            s"${hdfsMode.getClass.getSimpleName}Local(${hdfsMode.strict})"
          case _ =>
            s"${hdfsMode.getClass.getSimpleName}(${hdfsMode.strict})"
        }
        (m, defaultFs.getWorkingDirectory.toString)
      }
    }
    println(s"${Console.GREEN}#### Scalding mode: ${modeString}")
    println(s"#### User home: ${homeDir}${Console.RESET}")
  }

  private def modeHadoopConf: Configuration = {
    mode match {
      case hdfsMode: Hdfs => hdfsMode.jobConf
      case _ => new Configuration(false)
    }
  }

  /**
   * Access to Hadoop FsShell
   *
   * @param cmdArgs list of command line parameters for FsShell, one per method argument
   * @return
   */
  def fsShellExp(cmdArgs: String*): Int = {
    new FsShell(modeHadoopConf).run(cmdArgs.toArray)
  }

  /**
   * Access to Hadoop FsShell
   *
   * @param cmdLine command line parameters for FsShell as a single string
   * @return
   */
  def fsShell(cmdLine: String): Int = {
    new FsShell(modeHadoopConf).run(cmdLine.trim.split(" "))
  }

  /**
   * Configuration to use for REPL executions.
   *
   * To make changes, don't forget to assign back to this var:
   * config += "mapred.reduce.tasks" -> 2
   */
  var customConfig = Config.empty

  /* Using getter/setter here lets us get the correct defaults set by the mode
     (and read from command-line, etc) while still allowing the user to customize it */
  def config: Config = Config.defaultFrom(mode) ++ customConfig

  protected def shell: BaseScaldingShell = ScaldingShell

  /** Create config for execution. Tacks on a new jar for each execution. */
  private[scalding] def executionConfig: Config = {
    // Create a jar to hold compiled code for this REPL session in addition to
    // "tempjars" which can be passed in from the command line, allowing code
    // in the repl to be distributed for the Hadoop job to run.
    val replCodeJar: Option[java.io.File] = shell.createReplCodeJar()
    val tmpJarsConfig: Map[String, String] =
      replCodeJar match {
        case Some(jar) =>
          Map("tmpjars" -> {
            // Use tmpjars already in the configuration.
            config.get("tmpjars").map(_ + ",").getOrElse("")
              // And a jar of code compiled by the REPL.
              .concat("file://" + jar.getAbsolutePath)
          })
        case None =>
          // No need to add the tmpjars to the configuration
          Map()
      }
    config ++ tmpJarsConfig
  }

  /**
   * Sets the flow definition in implicit scope to an empty flow definition.
   */
  def resetFlowDef(): Unit = {
    flowDef = getEmptyFlowDef
  }

  /**
   * Gets a new, empty, flow definition.
   *
   * @return a new, empty flow definition.
   */
  def getEmptyFlowDef: FlowDef = {
    val fd = new FlowDef
    fd.setName("ScaldingShell")
    fd
  }

  /**
   * Runs this pipe as a Scalding job.
   *
   * Automatically cleans up the flowDef to include only sources upstream from tails.
   */
  def run(implicit fd: FlowDef, md: Mode): Option[JobStats] =
    ExecutionContext.newContext(executionConfig)(fd, md).waitFor match {
      case Success(stats) => Some(stats)
      case Failure(e) =>
        println("Flow execution failed!")
        e.printStackTrace()
        None
    }

  /*
   * Starts the Execution, but does not wait for the result
   */
  def asyncExecute[T](execution: Execution[T])(implicit ec: ConcurrentExecutionContext): Future[T] =
    execution.run(executionConfig, mode)

  /*
   * This runs the Execution[T] and waits for the result
   */
  def execute[T](execution: Execution[T]): T =
    execution.waitFor(executionConfig, mode).get
}

object ReplImplicits extends FieldConversions {

  /**
   * Converts a Cascading Pipe to a Scalding RichPipe. This method permits implicit conversions from
   * Pipe to RichPipe.
   *
   * @param pipe to convert to a RichPipe.
   * @return a RichPipe wrapping the specified Pipe.
   */
  implicit def pipeToRichPipe(pipe: Pipe): RichPipe = new RichPipe(pipe)

  /**
   * Converts a Scalding RichPipe to a Cascading Pipe. This method permits implicit conversions from
   * RichPipe to Pipe.
   *
   * @param richPipe to convert to a Pipe.
   * @return the Pipe wrapped by the specified RichPipe.
   */
  implicit def richPipeToPipe(richPipe: RichPipe): Pipe = richPipe.pipe

  /**
   * Converts a Source to a RichPipe. This method permits implicit conversions from Source to
   * RichPipe.
   *
   * @param source to convert to a RichPipe.
   * @return a RichPipe wrapping the result of reading the specified Source.
   */
  implicit def sourceToRichPipe(source: Source)(implicit flowDef: FlowDef, mode: Mode): RichPipe =
    RichPipe(source.read(flowDef, mode))

  /**
   * Converts an iterable into a Source with index (int-based) fields.
   *
   * @param iterable to convert into a Source.
   * @param setter implicitly retrieved and used to convert the specified iterable into a Source.
   * @param converter implicitly retrieved and used to convert the specified iterable into a Source.
   * @return a Source backed by the specified iterable.
   */
  implicit def iterableToSource[T](
    iterable: Iterable[T])(implicit setter: TupleSetter[T],
      converter: TupleConverter[T]): Source = {
    IterableSource[T](iterable)(setter, converter)
  }

  /**
   * Converts an iterable into a Pipe with index (int-based) fields.
   *
   * @param iterable to convert into a Pipe.
   * @param setter implicitly retrieved and used to convert the specified iterable into a Pipe.
   * @param converter implicitly retrieved and used to convert the specified iterable into a Pipe.
   * @return a Pipe backed by the specified iterable.
   */
  implicit def iterableToPipe[T](
    iterable: Iterable[T])(implicit setter: TupleSetter[T],
      converter: TupleConverter[T], flowDef: FlowDef, mode: Mode): Pipe = {
    iterableToSource(iterable)(setter, converter).read
  }

  /**
   * Converts an iterable into a RichPipe with index (int-based) fields.
   *
   * @param iterable to convert into a RichPipe.
   * @param setter implicitly retrieved and used to convert the specified iterable into a RichPipe.
   * @param converter implicitly retrieved and used to convert the specified iterable into a
   *     RichPipe.
   * @return a RichPipe backed by the specified iterable.
   */
  implicit def iterableToRichPipe[T](
    iterable: Iterable[T])(implicit setter: TupleSetter[T],
      converter: TupleConverter[T], flowDef: FlowDef, mode: Mode): RichPipe = {
    RichPipe(iterableToPipe(iterable)(setter, converter, flowDef, mode))
  }

  /**
   * Convert KeyedListLike to enriched ShellTypedPipe
   * (e.g. allows .snapshot to be called on Grouped, CoGrouped, etc)
   */
  implicit def keyedListLikeToShellTypedPipe[K, V, T[K, +V] <: KeyedListLike[K, V, T]](kll: KeyedListLike[K, V, T])(implicit state: BaseReplState): ShellTypedPipe[(K, V)] =
    new ShellTypedPipe(kll.toTypedPipe)(state)

  /**
   * Enrich TypedPipe for the shell
   * (e.g. allows .snapshot to be called on it)
   */
  implicit def typedPipeToShellTypedPipe[T](pipe: TypedPipe[T])(implicit state: BaseReplState): ShellTypedPipe[T] =
    new ShellTypedPipe[T](pipe)(state)

  /**
   * Enrich ValuePipe for the shell
   * (e.g. allows .toOption to be called on it)
   */
  implicit def valuePipeToShellValuePipe[T](pipe: ValuePipe[T])(implicit state: BaseReplState): ShellValuePipe[T] =
    new ShellValuePipe[T](pipe)(state)

}

object ReplState extends BaseReplState

/**
 * Implicit FlowDef and Mode, import in the REPL to have the global context implicitly
 * used everywhere.
 */
object ReplImplicitContext {
  /** Implicit execution context for using the Execution monad */
  implicit val executionContext: scala.concurrent.ExecutionContextExecutor = ConcurrentExecutionContext.global
  /** Implicit repl state used for ShellPipes */
  implicit def stateImpl: ReplState.type = ReplState
  /** Implicit flowDef for this Scalding shell session. */
  implicit def flowDefImpl: FlowDef = ReplState.flowDef
  /** Defaults to running in local mode if no mode is specified. */
  implicit def modeImpl: Mode = ReplState.mode
  implicit def configImpl: Config = ReplState.config
}
