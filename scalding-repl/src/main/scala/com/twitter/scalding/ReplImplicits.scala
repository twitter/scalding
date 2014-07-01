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

/**
 * Object containing various implicit conversions required to create Scalding flows in the REPL.
 * Most of these conversions come from the [[com.twitter.scalding.Job]] class.
 */
object ReplImplicits extends FieldConversions {
  /** Implicit flowDef for this Scalding shell session. */
  implicit var flowDef: FlowDef = getEmptyFlowDef
  /** Defaults to running in local mode if no mode is specified. */
  implicit var mode: Mode = com.twitter.scalding.Local(false)

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
   * Gets a job that can be used to run the data pipeline.
   *
   * @param args that should be used to construct the job.
   * @return a job that can be used to run the data pipeline.
   */
  private[scalding] def getJob(args: Args, inmode: Mode, inFlowDef: FlowDef): Job = new Job(args) {
    /**
     *  The flow definition used by this job, which should be the same as that used by the user
     *  when creating their pipe.
     */
    override val flowDef = inFlowDef

    override def mode = inmode

    /**
     * Obtains a configuration used when running the job.
     *
     * This overridden method uses the same configuration as a standard Scalding job,
     * but adds options specific to KijiScalding, including adding a jar containing compiled REPL
     * code to the distributed cache if the REPL is running.
     *
     * @return the configuration that should be used to run the job.
     */
    override def config: Map[AnyRef, AnyRef] = {
      // Use the configuration from Scalding Job as our base.
      val configuration: Map[AnyRef, AnyRef] = super.config

      /** Appends a comma to the end of a string. */
      def appendComma(str: Any): String = str.toString + ","

      // If the REPL is running, we should add tmpjars passed in from the command line,
      // and a jar of REPL code, to the distributed cache of jobs run through the REPL.
      val replCodeJar = ScaldingShell.createReplCodeJar()
      val tmpJarsConfig: Map[String, String] =
        if (replCodeJar.isDefined) {
          Map("tmpjars" -> {
            // Use tmpjars already in the configuration.
            configuration
              .get("tmpjars")
              .map(appendComma)
              .getOrElse("") +
              // And a jar of code compiled by the REPL.
              "file://" + replCodeJar.get.getAbsolutePath
          })
        } else {
          // No need to add the tmpjars to the configuration
          Map()
        }

      configuration ++ tmpJarsConfig
    }

  }

  /**
   * Runs this pipe as a Scalding job.
   *
   * Automatically cleans up the flowDef to include only sources upstream from tails.
   */
  def run(implicit flowDef: FlowDef) = {
    import Dsl.flowDefToRichFlowDef

    val (r, tryStats) = Execution.waitFor(mode, Config.default) {
      implicit ec: ExecutionContext =>
        ec.flowDef.mergeFrom(flowDef)
    }

    tryStats match {
      case Success(stats) => println(stats.toJson)
      case Failure(e) =>
        println("Flow execution failed!")
        e.printStackTrace()
    }

    r
  }

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
      converter: TupleConverter[T]): Pipe = {
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
      converter: TupleConverter[T]): RichPipe = {
    RichPipe(iterableToPipe(iterable)(setter, converter))
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

}
