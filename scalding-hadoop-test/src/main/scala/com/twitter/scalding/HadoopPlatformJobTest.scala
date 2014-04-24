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

import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.FileReader
import java.io.FileWriter
import java.util.jar.Attributes
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import java.util.jar.{Manifest => JarManifest}

import scala.collection.mutable.Buffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MiniMRCluster

import com.twitter.bijection._

object MakeJar {
  def apply(classDir: File, jarName: Option[String] = None): File = {
    val syntheticJar = new File(
      System.getProperty("java.io.tmpdir"),
      jarName.getOrElse(classDir.getAbsolutePath.replace("/", "_") + ".jar")
    )
    println("Creating synthetic jar: " + syntheticJar.getAbsolutePath) //TODO use logging
    val manifest = new JarManifest
    manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
    val target = new JarOutputStream(new FileOutputStream(syntheticJar), manifest)
    add(classDir, classDir, target)
    target.close()
    new File(syntheticJar.getAbsolutePath)
  }

  private[this] def add(parent: File, source: File, target: JarOutputStream) {
    val name = getRelativeFileBetween(parent, source).getOrElse(new File("")).getPath.replace("\\", "/")
    if (source.isDirectory) {
      if (!name.isEmpty) {
        val entry = new JarEntry(if (!name.endsWith("/")) name + "/" else name)
        entry.setTime(source.lastModified())
        target.putNextEntry(entry)
        target.closeEntry()
      }
      source.listFiles.foreach { add(parent, _, target) }
    } else {
      val entry = new JarEntry(name)
      entry.setTime(source.lastModified)
      target.putNextEntry(entry)
      val in = new BufferedInputStream(new FileInputStream(source))
      val buffer = new Array[Byte](1024)
      var count = in.read(buffer)
      while (count > -1) {
        target.write(buffer, 0, count)
        count = in.read(buffer)
      }
      target.closeEntry
      in.close()
    }
  }

  // Note that this assumes that parent and source are in absolute form if that's what we want
  @annotation.tailrec
  private[this] def getRelativeFileBetween(
      parent: File, source: File, result: List[String] = List.empty): Option[File] =
    Option(source) match {
      case Some(src) => {
        if (parent == src) {
          result.foldLeft(None: Option[File]) { (cum, part) =>
            Some(cum match {
              case Some(p) => new File(p, part)
              case None => new File(part)
            })
          }
        } else {
          getRelativeFileBetween(parent, src.getParentFile, src.getName :: result)
        }
      }
      case None => None
    }
}

object LocalCluster {
  def apply() = new LocalCluster()
}
class LocalCluster() {
  private var hadoop: Option[(MiniDFSCluster, MiniMRCluster, JobConf)] = None

  private def dfs = hadoop.getOrElse(throw new Exception("Hadoop has not been initialized, cannot get dfs"))._1
  private def cluster = hadoop.getOrElse(throw new Exception("Hadoop has not been initialized, cannot get cluster"))._2
  private def jobConf = hadoop.getOrElse(throw new Exception("Hadoop has not been initialized, cannot get jobConf"))._3
  private def fileSystem = dfs.getFileSystem

  private var classpath = Set[File]()

  def initialize(): this.type = {
    //TODO be cleaner about this
    System.setProperty("hadoop.log.dir", "log");
    new File(System.getProperty("hadoop.log.dir")).mkdirs(); // ignored

    //TODO need to make sure that this stuff is shutdown! Need that hook
    val conf = new Configuration
    val dfs = new MiniDFSCluster(conf, 4, true, null)
    val fileSystem = dfs.getFileSystem
    val cluster = new MiniMRCluster(4, fileSystem.getUri.toString, 1, null, null, new JobConf(conf))
    val mrJobConf = cluster.createJobConf()
    mrJobConf.setInt("mapred.submit.replication", 2);
    mrJobConf.set("mapred.map.max.attempts", "2");
    mrJobConf.set("mapred.reduce.max.attempts", "2");
    mrJobConf.set("mapred.child.java.opts", "-Xmx512m")
    mrJobConf.setInt("mapred.job.reuse.jvm.num.tasks", -1)
    mrJobConf.setInt("jobclient.completion.poll.interval", 50)
    mrJobConf.setInt("jobclient.progress.monitor.poll.interval", 50)
    mrJobConf.setMapSpeculativeExecution(false)
    mrJobConf.setReduceSpeculativeExecution(false)
    mrJobConf.set("mapreduce.user.classpath.first", "true")

    val jarsDir = new Path("/tmp/hadoop-test-lib")
    fileSystem.mkdirs(jarsDir)

    hadoop = Some(dfs, cluster, mrJobConf)

    //TODO I desperately want there to be a better way to do this. I'd love to be able to run ./sbt assembly and depend
    // on that, but I couldn't figure out how to make that work.
    val baseClassPath = List(
      getClass,
      classOf[JobConf],
      classOf[scala.ScalaObject],
      classOf[org.slf4j.LoggerFactory],
      classOf[com.twitter.scalding.Args],
      classOf[org.apache.log4j.LogManager],
      classOf[com.twitter.scalding.RichDate],
      classOf[cascading.tuple.TupleException],
      classOf[com.twitter.chill.Externalizer[_]],
      classOf[com.twitter.algebird.Semigroup[_]],
      classOf[com.twitter.chill.KryoInstantiator],
      classOf[org.jgrapht.ext.EdgeNameProvider[_]],
      classOf[org.apache.commons.lang.StringUtils],
      classOf[cascading.scheme.local.TextDelimited],
      classOf[org.apache.commons.logging.LogFactory],
      classOf[org.apache.commons.codec.binary.Base64],
      classOf[com.twitter.scalding.IntegralComparator],
      classOf[org.apache.commons.collections.Predicate],
      classOf[com.esotericsoftware.kryo.KryoSerializable],
      classOf[com.twitter.chill.hadoop.KryoSerialization],
      classOf[org.apache.commons.configuration.Configuration]
    ).map { addClassSourceToClassPath(_: Class[_]) }
    this
  }

  //TODO probably need a way to remove classes as well
  def addClassSourceToClassPath(clazz: Class[_]) {
    addFileToHadoopClassPath(getFileForClass(clazz))
  }

  def addFileToHadoopClassPath(resourceDir: File): Boolean = {
    if (classpath.contains(resourceDir)) {
      println("Already on Hadoop classpath: " + resourceDir)
      false
    } else {
      println("Not yet on Hadoop classpath: " + resourceDir)
      val localJarFile = if (resourceDir.isDirectory) MakeJar(resourceDir) else resourceDir
      val hdfsJarPath = new Path("/tmp/hadoop-test-lib/%s".format(localJarFile.getName))
      fileSystem.copyFromLocalFile(new Path("file://%s".format(localJarFile.getAbsolutePath)), hdfsJarPath)
      DistributedCache.addFileToClassPath(hdfsJarPath, jobConf, fileSystem)
      println("Added to Hadoop classpath: " + localJarFile)
      classpath += resourceDir
      true
    }
  }

  def getFileForClass(clazz: Class[_]): File = new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI)

  def mode: Mode = Hdfs(true, jobConf)

  def putFile(file: File, location: String): Boolean = {
    val hdfsLocation = new Path(location)
    val exists = fileSystem.exists(hdfsLocation)
    if (!exists) FileUtil.copy(file, fileSystem, hdfsLocation, false, jobConf)
    exists
  }

  //TODO is there a way to know if we need to wait on anything to shut down, etc?
  def shutdown() {
    fileSystem.close()
    dfs.shutdown()
    cluster.shutdown()
    hadoop = None
  }
}

object HadoopPlatformJobTest {
  def apply(cons : (Args) => Job, cluster: LocalCluster) = {
    new HadoopPlatformJobTest(cons, cluster)
  }
}

/**
 * This class is used to construct unit tests in scalding which
 * use Hadoop's MiniCluster to more fully simulate and test
 * the logic which is deployed in a job.
 */
//TODO what should the relationship of this with JobTest be?
//TODO should we factor out the args stuff?
class HadoopPlatformJobTest(cons : (Args) => Job, cluster: LocalCluster) {
  private var argsMap = Map[String, List[String]]()
  private val dataToCreate = Buffer[(String, Seq[String])](("dummyInput", Seq("dummyLine")))
  private val sourceWriters = Buffer[Args => Job]()
  //private val sourceReaders = Buffer[Args => Job]()
  private val sourceReaders = Buffer[Mode => Unit]()

  cluster.addClassSourceToClassPath(cons.getClass)

  def arg(inArg: String, value: List[String]) = {
    argsMap += inArg -> value
    this
  }

  def arg(inArg: String, value: String) = {
    argsMap += inArg -> List(value)
    this
  }

  def source[T: Manifest](location: String, data: Seq[T]): this.type = source(TypedTsv[T](location), data)

  def source[T](out: TypedSink[T], data: Seq[T]): this.type = {
    sourceWriters.+=({ args: Args =>
      new Job(args) {
        TypedPipe.from(TypedTsv[String]("dummyInput")).flatMap { _ => data }.write(out)
      }
    })
    this
  }

  def sink[T: Manifest](location: String)(toExpect: Seq[T] => Unit): this.type = sink(TypedTsv[T](location))(toExpect)

  def sink[T](in: Mappable[T])(toExpect: Seq[T] => Unit): this.type = {
    sourceReaders.+=(mode => toExpect(in.toIterator(mode).toSeq))
    this
  }

  private def createSources() {
    dataToCreate foreach { case (location, lines) =>
      val tmpFile = File.createTempFile("hadoop_platform", "job_test")
      tmpFile.deleteOnExit()
      if (!lines.isEmpty) {
        val os = new BufferedWriter(new FileWriter(tmpFile))
        os.write(lines.head)
        lines.tail.foreach { str =>
          os.newLine()
          os.write(str)
        }
        os.close()
      }
      cluster.putFile(tmpFile, location)
      tmpFile.delete()
    }

    sourceWriters.foreach { cons => runJob(initJob(cons)) }
  }

  private def checkSinks() {
    println("CHECKING SINKS") //TODO remove
    sourceReaders.foreach { _(cluster.mode) }
  }

  //WE NEED TO SHUT DOWN THE MINICLUSTER
  def run {
    createSources()
    runJob(initJob(cons))
    checkSinks()
  }

  private def initJob(cons: Args => Job): Job = cons(Mode.putMode(cluster.mode, new Args(argsMap)))

  @annotation.tailrec
  private final def runJob(job: Job) {
    job.run
    job.clear
    job.next match {
      case Some(nextJob) => runJob(nextJob)
      case None => ()
    }
  }
}
