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
import typed.KeyedListLike
import scala.util.{ Failure, Success }
import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext }

/**
 * Object containing various implicit conversions required to create Scalding flows in the REPL.
 * Most of these conversions come from the [[com.twitter.scalding.Job]] class.
 */
object ReplImplicits extends FieldConversions {

  /** Implicit flowDef for this Scalding shell session. */
  var flowDef: FlowDef = getEmptyFlowDef
  /** Defaults to running in local mode if no mode is specified. */
  var mode: Mode = com.twitter.scalding.Local(false)

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
  def config_=(c: Config) { customConfig = c }

  /** Create config for execution. Tacks on a new jar for each execution. */
  private[scalding] def executionConfig: Config = {
    // Create a jar to hold compiled code for this REPL session in addition to
    // "tempjars" which can be passed in from the command line, allowing code
    // in the repl to be distributed for the Hadoop job to run.
    val replCodeJar = ScaldingShell.createReplCodeJar()
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
  def resetFlowDef() {
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
  implicit def sourceToRichPipe(source: Source): RichPipe = RichPipe(source.read(flowDef, mode))

  /**
   * Converts a Source to a Pipe. This method permits implicit conversions from Source to Pipe.
   *
   * @param source to convert to a Pipe.
   * @return a Pipe that is the result of reading the specified Source.
   */
  implicit def sourceToPipe(source: Source): Pipe = source.read(flowDef, mode)

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
      converter: TupleConverter[T], fd: FlowDef, md: Mode): Pipe = {
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
      converter: TupleConverter[T], fd: FlowDef, md: Mode): RichPipe = {
    RichPipe(iterableToPipe(iterable)(setter, converter, fd, md))
  }

  /**
   * Convert KeyedListLike to enriched ShellTypedPipe
   * (e.g. allows .snapshot to be called on Grouped, CoGrouped, etc)
   */
  implicit def keyedListLikeToShellTypedPipe[K, V, T[K, +V] <: KeyedListLike[K, V, T]](kll: KeyedListLike[K, V, T]) = new ShellTypedPipe(kll.toTypedPipe)

  /**
   * Enrich TypedPipe for the shell
   * (e.g. allows .snapshot to be called on it)
   */
  implicit def typedPipeToShellTypedPipe[T](pipe: TypedPipe[T]): ShellTypedPipe[T] =
    new ShellTypedPipe[T](pipe)

  /**
   * Enrich ValuePipe for the shell
   * (e.g. allows .toOption to be called on it)
   */
  implicit def valuePipeToShellValuePipe[T](pipe: ValuePipe[T]): ShellValuePipe[T] =
    new ShellValuePipe[T](pipe)

}

/**
 * Implicit FlowDef and Mode, import in the REPL to have the global context implicitly
 * used everywhere.
 */
object ReplImplicitContext {
  /** Implicit flowDef for this Scalding shell session. */
  implicit def flowDefImpl = ReplImplicits.flowDef
  /** Defaults to running in local mode if no mode is specified. */
  implicit def modeImpl = ReplImplicits.mode
  implicit def configImpl = ReplImplicits.config
}
