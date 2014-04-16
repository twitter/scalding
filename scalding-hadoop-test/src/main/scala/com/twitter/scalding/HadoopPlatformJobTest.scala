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
//TODO this should perhaps be in a platform-test package

import java.io.File

import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MiniMRCluster

import com.twitter.bijection._

object HadoopPlatformJobTest {
  def apply(cons : (Args) => Job) = {
    new HadoopPlatformJobTest(cons)
  }
}

/**
 * This class is used to construct unit tests in scalding which
 * use Hadoop's MiniCluster to more fully simulate and test
 * the logic which is deployed in a job.
 */
//TODO what should the relationship of this with JobTest be?
//TODO should we factor out the args stuff?
class HadoopPlatformJobTest(cons : (Args) => Job) {
  private var argsMap = Map[String, List[String]]()

  //TODO we could potentially share these beteween runs?
  private lazy val (dfs, fileSystem, cluster, jobConf) = {
    val initJobConf = new JobConf
    val dfs = new MiniDFSCluster(initJobConf, 4, true, null)
    val fileSystem = dfs.getFileSystem()
    val cluster = new MiniMRCluster(4, fileSystem.getUri().toString(), 1, null, null, initJobConf)
    val mrJobConf = mr.createJobConf()
    mrJobConf.set("mapred.child.java.opts", "-Xmx512m")
    mrJobConf.setInt("mapred.job.reuse.jvm.num.tasks", -1)
    mrJobConf.setInt("jobclient.completion.poll.interval", 50)
    mrJobConf.setInt("jobclient.progress.monitor.poll.interval", 50)
    mrJobConf.setMapSpeculativeExecution(false)
    mrJobConf.setReduceSpeculativeExecution(false)
    mrJobConf.setNumMapTasks(mapTasks)
    mrJobConf.setNumReduceTasks(reduceTasks)
    (dfs, fileSystem, cluster, mrJobConf)
  }

  def arg(inArg: String, value: List[String]) = {
    argsMap += inArg -> value
    this
  }

  def arg(inArg: String, value: String) = {
    argsMap += inArg -> List(value)
    this
  }

  def createData[T](location: String, data: T)(implicit codec: Codec[T]): Boolean = {
    val inputFile = File.createTempFile("hadoop_platform", "job_test")
    inputFile.deleteOnExit
    val baos = new ByteArrayOutputStream(new FileOutputStream(inputFile))
    val bytes = codec(data)
    baos.write(bytes, 0, bytes.length)
    baos.close()
    FileUtil.copy(inputFile, fileSystem, new Path(location), false, jobConf)
    inputFile.delete()
  }

  def run {
    runJob(initJob())
  }

  private def initJob(): Job = {
    val args = new Args(argsMap)
    // Construct a job.
    cons(Mode.putMode(Hdfs(false, mrJobConf), args))
  }

  @tailrec
  private final def runJob(job: Job) {
    job.run
    job.clear
    job.next.foreach { runJob(_) }
    //TODO we need to shut down this guy!
  }
}
