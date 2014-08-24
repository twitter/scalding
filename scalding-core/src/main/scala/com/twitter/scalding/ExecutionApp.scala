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

/*
 * Make an object that extend this trait, and you can run
 * it as a normal java application.
 */
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
    val pattern = "-D([^=]+)=([^\\s]+)".r
    val (hadoopArgs, nonHadoop) = inputArgs.partition(pattern.findFirstIn(_).isDefined)
    val hconf = new Configuration
    // This has the side-effect of mutating the hconf
    val unparsed = (new GenericOptionsParser(hconf, hadoopArgs)).getRemainingArgs
    val args = Args(nonHadoop ++ unparsed)
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

  def main(args: Array[String]) {
    config(args) match {
      case (conf, mode) => job.waitFor(conf, mode).get
    }
  }
}
