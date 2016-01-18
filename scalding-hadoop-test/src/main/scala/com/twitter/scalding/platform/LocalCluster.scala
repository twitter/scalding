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
package com.twitter.scalding.platform

import com.twitter.scalding._

import java.io.{ File, RandomAccessFile }
import java.nio.channels.FileLock

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.filecache.DistributedCache
import org.apache.hadoop.fs.{ FileUtil, Path }
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.mapred.{ JobConf, MiniMRCluster }
import org.slf4j.LoggerFactory
import org.slf4j.impl.Log4jLoggerAdapter

object LocalCluster {
  private final val HADOOP_CLASSPATH_DIR = new Path("/tmp/hadoop-classpath-lib")
  private final val MUTEX = new RandomAccessFile("NOTICE", "rw").getChannel

  def apply() = new LocalCluster()
}

class LocalCluster(mutex: Boolean = true) {
  org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("BlockStateChange").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("SecurityLogger").setLevel(org.apache.log4j.Level.ERROR)

  private val LOG = LoggerFactory.getLogger(getClass)

  private var hadoop: Option[(MiniDFSCluster, MiniMRCluster, JobConf)] = None
  private def getHadoop = hadoop.getOrElse(throw new Exception("Hadoop has not been initialized"))

  private def dfs = getHadoop._1
  private def cluster = getHadoop._2
  private def jobConf = getHadoop._3
  private def fileSystem = dfs.getFileSystem

  private var classpath = Set[File]()
  private var lock: Option[FileLock] = None

  // The Mini{DFS,MR}Cluster does not make it easy or clean to have two different processes
  // running without colliding. Thus we implement our own mutex. Mkdir should be atomic so
  // there should be no race. Just to be careful, however, we make sure that the file
  // is what we expected, or else we fail.
  private[this] def acquireMutex() {
    LOG.debug("Attempting to acquire mutex")
    lock = Some(LocalCluster.MUTEX.lock())
    LOG.debug("Mutex file acquired")
  }

  private[this] def releaseMutex() {
    LOG.debug("Releasing mutex")
    lock.foreach { _.release() }
    LOG.debug("Mutex released")
    lock = None
  }

  /**
   * Start up the local cluster instance.
   *
   * @param inConf  override default configuration
   */
  def initialize(inConf: Config = Config.empty): this.type = {
    if (mutex) {
      acquireMutex()
    }

    if (Option(System.getProperty("hadoop.log.dir")).isEmpty) {
      System.setProperty("hadoop.log.dir", "build/test/logs")
    }
    new File(System.getProperty("hadoop.log.dir")).mkdirs()

    val conf = new Configuration
    val dfs = new MiniDFSCluster(conf, 4, true, null)
    val fileSystem = dfs.getFileSystem
    val cluster = new MiniMRCluster(4, fileSystem.getUri.toString, 1, null, null, new JobConf(conf))
    val mrJobConf = cluster.createJobConf()
    mrJobConf.setInt("mapred.submit.replication", 2)
    mrJobConf.set("mapred.map.max.attempts", "2")
    mrJobConf.set("mapred.reduce.max.attempts", "2")
    mrJobConf.set("mapred.child.java.opts", "-Xmx512m")
    mrJobConf.setInt("mapred.job.reuse.jvm.num.tasks", -1)
    mrJobConf.setInt("mapreduce.client.completion.pollinterval", 20)
    mrJobConf.setInt("mapreduce.client.progressmonitor.pollinterval", 20)
    mrJobConf.setInt("ipc.ping.interval", 500)
    mrJobConf.setInt("dfs.client.socket-timeout", 50)
    mrJobConf.set("mapreduce.job.ubertask.enable", "true")
    mrJobConf.setInt("mapreduce.job.ubertask.maxmaps", 500)
    mrJobConf.setInt("mapreduce.job.ubertask.maxreduces", 500)
    mrJobConf.setInt("ipc.client.connection.maxidletime", 50)

    mrJobConf.setMapSpeculativeExecution(false)
    mrJobConf.setReduceSpeculativeExecution(false)
    mrJobConf.set("mapreduce.user.classpath.first", "true")

    LOG.debug("Creating directory to store jars on classpath: " + LocalCluster.HADOOP_CLASSPATH_DIR)
    fileSystem.mkdirs(LocalCluster.HADOOP_CLASSPATH_DIR)

    // merge in input configuration
    inConf.toMap.foreach{ case (k, v) => mrJobConf.set(k, v) }

    hadoop = Some(dfs, cluster, mrJobConf)

    //TODO I desperately want there to be a better way to do this. I'd love to be able to run ./sbt assembly and depend
    // on that, but I couldn't figure out how to make that work.
    val baseClassPath = List(
      getClass,
      classOf[JobConf],
      classOf[Option[_]],
      classOf[LoggerFactory],
      classOf[Log4jLoggerAdapter],
      classOf[org.apache.hadoop.net.StaticMapping],
      classOf[org.apache.hadoop.yarn.server.MiniYARNCluster],
      classOf[com.twitter.scalding.Args],
      classOf[org.apache.log4j.LogManager],
      classOf[com.twitter.scalding.RichDate],
      classOf[cascading.tuple.TupleException],
      classOf[com.twitter.chill.Externalizer[_]],
      classOf[com.twitter.chill.algebird.AveragedValueSerializer],
      classOf[com.twitter.algebird.Semigroup[_]],
      classOf[com.twitter.chill.KryoInstantiator],
      //classOf[org.jgrapht.ext.EdgeNameProvider[_]],
      classOf[org.apache.commons.lang.StringUtils],
      classOf[cascading.scheme.local.TextDelimited],
      classOf[org.apache.commons.logging.LogFactory],
      classOf[org.apache.commons.codec.binary.Base64],
      classOf[com.twitter.scalding.IntegralComparator],
      classOf[org.apache.commons.collections.Predicate],
      classOf[com.esotericsoftware.kryo.KryoSerializable],
      classOf[com.twitter.chill.hadoop.KryoSerialization],
      classOf[com.twitter.maple.tap.TupleMemoryInputFormat],
      classOf[org.apache.commons.configuration.Configuration]).foreach { addClassSourceToClassPath(_) }
    this
  }

  def addClassSourceToClassPath[T](clazz: Class[T]) {
    addFileToHadoopClassPath(getFileForClass(clazz))
  }

  def addFileToHadoopClassPath(resourceDir: File): Boolean =
    if (classpath.contains(resourceDir)) {
      LOG.debug("Already on Hadoop classpath: " + resourceDir)
      false
    } else {
      LOG.debug("Not yet on Hadoop classpath: " + resourceDir)
      val localJarFile = if (resourceDir.isDirectory) MakeJar(resourceDir) else resourceDir
      val hdfsJarPath = new Path(LocalCluster.HADOOP_CLASSPATH_DIR, localJarFile.getName)
      fileSystem.copyFromLocalFile(new Path("file://%s".format(localJarFile.getAbsolutePath)), hdfsJarPath)
      DistributedCache.addFileToClassPath(hdfsJarPath, jobConf, fileSystem)
      LOG.debug("Added to Hadoop classpath: " + localJarFile)
      classpath += resourceDir
      true
    }

  private def getFileForClass[T](clazz: Class[T]): File =
    new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI)

  def mode: Mode = {
    Mode.setDefaultFabricFromClasspath(jobConf)
    Hdfs(true, jobConf)
  }

  def putFile(file: File, location: String): Boolean = {
    val hdfsLocation = new Path(location)
    val exists = fileSystem.exists(hdfsLocation)
    if (!exists) FileUtil.copy(file, fileSystem, hdfsLocation, false, jobConf)
    exists
  }

  //TODO is there a way to know if we need to wait on anything to shut down, etc?
  def shutdown() {
    hadoop.foreach {
      case (dfs, mr, _) =>
        dfs.shutdown()
        mr.shutdown()
    }
    hadoop = None
    if (mutex) {
      releaseMutex()
    }
  }
}
