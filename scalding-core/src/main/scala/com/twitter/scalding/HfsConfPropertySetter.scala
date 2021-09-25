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

import cascading.tap.SinkMode
import org.apache.hadoop.mapred.JobConf
import cascading.flow.FlowProcess
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.OutputCollector
import cascading.scheme.Scheme
import cascading.tap.hadoop.Hfs
import com.twitter.scalding.tap.ScaldingHfs

private[scalding] class ConfPropertiesHfsTap(
  sourceConfig: Config,
  sinkConfig: Config,
  scheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _],
  stringPath: String,
  sinkMode: SinkMode) extends ScaldingHfs(scheme, stringPath, sinkMode) {
  override def sourceConfInit(process: FlowProcess[JobConf], conf: JobConf): Unit = {
    sourceConfig.toMap.foreach {
      case (k, v) =>
        conf.set(k, v)
    }
    super.sourceConfInit(process, conf)
  }

  override def sinkConfInit(process: FlowProcess[JobConf], conf: JobConf): Unit = {
    sinkConfig.toMap.foreach {
      case (k, v) =>
        conf.set(k, v)
    }
    super.sinkConfInit(process, conf)
  }
}

/*
 * The HfsConfPropertySetter can be added to sources to allow close in changes
 * to the Hadoop configuration properties for a source/sink in the flow.
 * Operations like changing the split sizes can be done here.
 *
 * Changes here however will not show up in the hadoop UI
 */
trait HfsConfPropertySetter extends HfsTapProvider {
  @deprecated("Tap config is deprecated, use sourceConfig or sinkConfig directly. In cascading configs applied to sinks can leak to sources in the step writing to the sink.", "0.17.0")
  def tapConfig: Config = Config.empty

  def sourceConfig: Config = Config.empty
  def sinkConfig: Config = Config.empty

  override def createHfsTap(
    scheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _],
    path: String,
    sinkMode: SinkMode): Hfs = {
    // Deprecation handling
    val (srcCfg, sinkCfg) = if (sourceConfig == Config.empty && sinkConfig == Config.empty) {
      (tapConfig, tapConfig)
    } else {
      (sourceConfig, sinkConfig)
    }
    new ConfPropertiesHfsTap(srcCfg, sinkCfg, scheme, path, sinkMode)
  }
}
