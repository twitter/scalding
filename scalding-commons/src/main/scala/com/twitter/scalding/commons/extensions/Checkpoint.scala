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

package com.twitter.scalding.commons.extensions

import com.twitter.scalding._
import com.twitter.scalding.Dsl._

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import org.slf4j.{ Logger, LoggerFactory => LogManager }

/**
 * Checkpoint provides a simple mechanism to read and write intermediate results
 * from a Scalding flow to HDFS.
 *
 * Checkpoints are useful for debugging one part of a long flow, when you would
 * otherwise have to run many steps to get to the one you care about.  To enable
 * checkpoints, sprinkle calls to Checkpoint() throughout your flow, ideally
 * after expensive steps.
 *
 * When checkpoints are enabled, each Checkpoint() looks for a checkpoint file
 * on HDFS.  If it exists we read results from the file; otherwise we execute
 * the flow and write the results to the file.  When checkpoints are disabled,
 * the flow is always executed and the results are never stored.
 *
 * Each call to Checkpoint() takes the checkpoint name, as well as the types and
 * names of the expected fields.  A sample invocation might look like this:
 *   val pipe = Checkpoint[(Long, String, Long)](
 *         "clicks", ('tweetId, 'clickUrl, 'clickCount)) { ... }
 * where { ... } contains a flow which computes the result.
 *
 * Most checkpoint parameters are specified via command-line flags:
 * --checkpoint.clobber: if true, recompute and overwrite any existing
 *     checkpoint files.
 * --checkpoint.clobber.<name>: override clobber for the given checkpoint.
 * --checkpoint.file: specifies a filename prefix to use for checkpoint files.
 *     If blank, checkpoints are disabled; otherwise the file for checkpoint
 *     <name> is <prefix>_<name>.
 * --checkpoint.file.<name>: override --checkpoint.file for the given
 *     checkpoint; specifies the whole filename, not the prefix.
 * --checkpoint.format: specifies a file format, either sequencefile or tsv.
 *     Default is sequencefile for HDFS, tsv for local.
 * --checkpoint.format.<name>: specifies file format for the given checkpoint.
 *
 * @author Mike Jahr
 */

object Checkpoint {
  private val LOG: Logger = LogManager.getLogger(this.getClass)

  /**
   * Type parameters:
   *   A:               tuple of result types
   *
   * Parameters:
   *   checkpointName:  name of the checkpoint
   *   resultFields:    tuple of result field names
   *   flow:            a function to run a flow to compute the result
   *
   * Implicit parameters:
   *   args:    provided by com.twitter.pluck.job.TwitterJob
   *   mode:    provided by com.twitter.scalding.Job
   *   flowDef: provided by com.twitter.scalding.Job
   *   conv:    provided by com.twitter.scalding.TupleConversions
   *   setter:  provided by com.twitter.scalding.TupleConversions
   */
  def apply[A](checkpointName: String, resultFields: Fields)(flow: => Pipe)(implicit args: Args, mode: Mode, flowDef: FlowDef,
    conv: TupleConverter[A], setter: TupleSetter[A]): Pipe = {
    conv.assertArityMatches(resultFields)
    setter.assertArityMatches(resultFields)

    // The filename to use for this checkpoint, or None if the checkpoint is
    // disabled.
    val filename: Option[String] = getFilename(checkpointName)
    val format: String = getFormat(checkpointName)

    filename match {
      case Some(name) if hasInput(checkpointName, name) =>
        // We have checkpoint input; read the file instead of executing the flow.
        LOG.info(s"""Checkpoint "${checkpointName}": reading ${format} input from "${name}"""")
        getSource(format, name)
          .read
          .mapTo(List.range(0, resultFields.size) -> resultFields)((x: A) => x)(conv, setter)
      // We don't have checkpoint input; execute the flow and project to the
      // requested fields.
      case Some(name) =>
        val pipe = flow.project(resultFields)

        // Write the checkpoint output.
        LOG.info(s"""Checkpoint "${checkpointName}": writing ${format} output to "${name}"""")
        pipe.write(getSource(format, name))
      case None =>
        flow.project(resultFields)
    }
  }

  // Wrapper for Checkpoint when using a TypedPipe
  def apply[A](checkpointName: String)(flow: => TypedPipe[A])(implicit args: Args, mode: Mode, flowDef: FlowDef,
    conv: TupleConverter[A], setter: TupleSetter[A]): TypedPipe[A] = {
    val rPipe = apply(checkpointName, Dsl.intFields(0 until conv.arity)) {
      flow.toPipe(Dsl.intFields(0 until conv.arity))
    }
    TypedPipe.from[A](rPipe, Dsl.intFields(0 until conv.arity))
  }

  // Helper class for looking up checkpoint arguments, either the base value from
  // --checkpoint.<argname> or the override value from
  // --checkpoint.<argname>.<checkpointname>
  // TODO(mjahr): maybe move this to scalding.Args
  private case class CheckpointArg(checkpointName: String, argName: String)(implicit args: Args) {
    val baseValue: Option[String] =
      args.optional("checkpoint." + argName)
    val overrideValue: Option[String] =
      args.optional("checkpoint." + argName + "." + checkpointName)
    def value: Option[String] =
      if (overrideValue.isDefined) {
        overrideValue
      } else {
        baseValue
      }
    def isTrue: Boolean = value.exists { _.toLowerCase != "false" }
  }

  // Returns the filename to use for the given checkpoint, or None if this
  // checkpoint is disabled.
  private def getFilename(checkpointName: String)(implicit args: Args, mode: Mode): Option[String] = {
    val fileArg = CheckpointArg(checkpointName, "file")
    if (fileArg.overrideValue.isDefined) {
      // The flag "--checkpoint.file.<name>=<filename>" is present; use its
      // value as the filename.
      fileArg.overrideValue
    } else {
      fileArg.baseValue.map { value =>
        // The flag "--checkpoint.file=<prefix>"; use "<prefix>_<name>" as the
        // filename.
        value + "_" + checkpointName
      }
    }
  }

  // Returns a format for the checkpoint.  The format of the source is
  // determined by the flag --checkpoint.format, and defaults to SequenceFile.
  private def getFormat(checkpointName: String)(implicit args: Args, mode: Mode): String = {
    val defaultFormat = mode match {
      case Hdfs(_, _) | HadoopTest(_, _) => "sequencefile"
      case _ => "tsv"
    }
    CheckpointArg(checkpointName, "format").value.getOrElse(defaultFormat).toLowerCase
  }

  // Returns a source for the checkpoint in the given format.
  private def getSource(format: String, filename: String)(implicit mode: Mode): Source = {
    format match {
      case "sequencefile" => SequenceFile(filename)
      case "tsv" => Tsv(filename)
      case _ => sys.error("Invalid value for --checkpoint.format: " + format)
    }
  }

  // Returns true if the given checkpoint file exists and should be read.
  private def hasInput(checkpointName: String, filename: String)(implicit args: Args, mode: Mode): Boolean = {
    !CheckpointArg(checkpointName, "clobber").isTrue && mode.fileExists(filename)
  }
}
