/*
Copyright 2014 Twitter, Inc.

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

/*
 * We will explicitly import any non-hadoop names
 */
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser

case class HadoopArgs(toArray: Array[String])

case class NonHadoopArgs(toArray: Array[String])

/*
 * Make an object that extend this trait, and you can run
 * it as a normal java application.
 */
object ExecutionApp {
  /*
   * Anything that looks like hadoop args, use
   * the hadoop arg parser, otherwise, scalding.
   */

  private[this] val dArgPattern = "-D([^=]+)=([^\\s]+)".r

  private[this] val hadoopReservedArgs = List("-fs", "-jt", "-files", "-libjars", "-archives")

  def extractUserHadoopArgs(args: Array[String]): (HadoopArgs, NonHadoopArgs) = {

    val argsWithLibJars = ExpandLibJarsGlobs(args)

    // This adds a look back mechanism to match on other hadoop args we need to support
    // currently thats just libjars
    val (hadoopArgs, tmpNonHadoop, finalLast) =
      argsWithLibJars.foldLeft(Array[String](), Array[String](), Option.empty[String]) {
        // Current is a -D, so store the last in non hadoop, and add current to hadoop args
        case ((hadoopArgs, nonHadoop, Some(l)), current) if dArgPattern.findFirstIn(current).isDefined =>
          (hadoopArgs :+ current, nonHadoop :+ l, None)
        // Current is a -D, but no last to concern with, and add current to hadoop args
        case ((hadoopArgs, nonHadoop, None), current) if dArgPattern.findFirstIn(current).isDefined =>
          (hadoopArgs :+ current, nonHadoop, None)
        // Current is ignored, but last was hadoop reserved arg so store them both in the hadoop args
        case ((hadoopArgs, nonHadoop, Some(x)), current) if hadoopReservedArgs.contains(x) =>
          (hadoopArgs ++ Array(x, current), nonHadoop, None)
        // Have a last but nothing matches current. So store last in non-hadoop and current in the last holder
        case ((hadoopArgs, nonHadoop, Some(l)), current) =>
          (hadoopArgs, nonHadoop :+ l, Some(current))
        // Have no last, and nothing matches. So just store current in the last spot
        case ((hadoopArgs, nonHadoop, None), current) =>
          (hadoopArgs, nonHadoop, Some(current))
      }
    // We can have something left in the last bucket, so extract it.
    val nonHadoop = finalLast match {
      case Some(x) => tmpNonHadoop :+ x
      case None => tmpNonHadoop
    }

    // Throwaway hadoop config
    // see which of our hadoop config args are not ones
    val unparsed = (new GenericOptionsParser(new Configuration, hadoopArgs)).getRemainingArgs

    (HadoopArgs(hadoopArgs.filter(!unparsed.contains(_))), NonHadoopArgs(nonHadoop ++ unparsed))
  }
}

trait ExecutionApp extends java.io.Serializable {
  def job: Execution[Unit]

  /**
   * The first argument should be the mode name (hdfs or local)
   *
   * The default for this is to parse all hadoop arguments
   * and put them into the config. Any unparsed hadoop
   * arguments are put into the Args.
   */
  def config(inputArgs: Array[String]): (Config, Mode) = {
    /*
     * Anything that looks like hadoop args, use
     * the hadoop arg parser, otherwise, scalding.
     */
    val (hadoopArgs, nonHadoop) = ExecutionApp.extractUserHadoopArgs(inputArgs)
    val hconf = new Configuration
    // This has the side-effect of mutating the hconf
    new GenericOptionsParser(hconf, hadoopArgs.toArray)
    val args = Args(nonHadoop.toArray)
    val mode = Mode(args, hconf)
    val config = Config.hadoopWithDefaults(hconf).setArgs(args)
    /*
     * Make sure the hadoop config is set in sync with the config
     * which should not matter for execution, but especially legacy
     * code that accesses the jobConf is the Hdfs class, we keep
     * it in sync.
     */
    config.toMap.foreach { case (k, v) => hconf.set(k, v) }

    (config, mode)
  }

  def main(args: Array[String]): Unit = {
    config(args) match {
      case (conf, mode) => job.waitFor(conf, mode).get
    }
  }
}
