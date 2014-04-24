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
  private val sourceWriters = Buffer[Args => Job]()
  private val expectations = Buffer[(String, Seq[String] => Unit)]()

  private var hadoop: Option[(MiniDFSCluster, MiniMRCluster, JobConf)] = None

  private def dfs = hadoop.getOrElse(throw new Exception("Hadoop has not been initialized, cannot get dfs"))._1
  private def cluster = hadoop.getOrElse(throw new Exception("Hadoop has not been initialized, cannot get cluster"))._2
  private def jobConf = hadoop.getOrElse(throw new Exception("Hadoop has not been initialized, cannot get jobConf"))._3
  private def fileSystem = dfs.getFileSystem

  //TODO we could potentially share these beteween runs?
  //TODO need to properly shutdown!
  def initializeCluster() {
    //TODO be cleaner about this
    System.setProperty("hadoop.log.dir", "log");
    new File(System.getProperty("hadoop.log.dir")).mkdirs(); // ignored

    //TODO need to make sure that this stuff is shutdown! Need that hook
    val conf = new Configuration
    val dfs = new MiniDFSCluster(conf, 4, true, null)
    val fileSystem = dfs.getFileSystem
    val cluster = new MiniMRCluster(4, fileSystem.getUri.toString, 1, null, null, new JobConf(conf))
    val mrJobConf = cluster.createJobConf()
    mrJobConf.set("mapred.child.java.opts", "-Xmx512m")
    mrJobConf.setInt("mapred.job.reuse.jvm.num.tasks", -1)
    mrJobConf.setInt("jobclient.completion.poll.interval", 50)
    mrJobConf.setInt("jobclient.progress.monitor.poll.interval", 50)
    mrJobConf.setMapSpeculativeExecution(false)
    mrJobConf.setReduceSpeculativeExecution(false)
    mrJobConf.set("mapreduce.user.classpath.first", "true")

    //TODO I desparately want there to be a better way to do this. I'd love to be able to run ./sbt assembly and depend
    // on that, but I couldn't figure out how to make that work.
    val myjars = List(
      cons.getClass,
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
      classOf[org.apache.commons.logging.LogFactory],
      classOf[org.apache.commons.codec.binary.Base64],
      classOf[com.twitter.scalding.IntegralComparator],
      classOf[org.apache.commons.collections.Predicate],
      classOf[com.esotericsoftware.kryo.KryoSerializable],
      classOf[com.twitter.chill.hadoop.KryoSerialization],
      classOf[org.apache.commons.configuration.Configuration]
    ).map { clazz: Class[_] => new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI) }.toSet

    // Set up cluster classpath:
    val jarsDir = new Path("/tmp/hadoop-test-lib")
    fileSystem.mkdirs(jarsDir)

    //TODO this is currently copying over all the files, we want to copy over the jars themselves, should be much faster
    // copy jars over to hdfs and add to classpath:
    myjars.foreach { jar =>
      println("Adding to distributed classpath: " + jar.getAbsolutePath)
      val localJarFile = if (jar.isDirectory) {
        //TODO needs to be put in a temporary directory and deleted afterwards
        val jarName = jar.getAbsolutePath.replace("/", "_") + ".jar"
        val syntheticJar = new File(System.getProperty("java.io.tmpdir"), jarName)
        println("Creating synthetic jar: " + syntheticJar.getAbsolutePath)
        val manifest = new JarManifest
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0")
        val target = new JarOutputStream(new FileOutputStream(syntheticJar), manifest)
        add(jar, jar, target)
        target.close()
        new File(syntheticJar.getAbsolutePath)
      } else {
        new File(jar.getAbsolutePath)
      }
      val hdfsJarPath = new Path("/tmp/hadoop-test-lib/%s".format(localJarFile.getName))
      fileSystem.copyFromLocalFile(new Path("file://%s".format(localJarFile.getAbsolutePath)), hdfsJarPath)
      DistributedCache.addFileToClassPath(hdfsJarPath, mrJobConf, fileSystem)
    }

    hadoop = Some(dfs, cluster, mrJobConf)
  }

// Note that this assumes that parent and source are in absolute form if that's what we want
@annotation.tailrec
private def getRelativeFileBetween(parent: File, source: File, result: List[String] = List.empty): Option[File] =
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

//TODO needs to name things properly
// ie /Users/jcoveney/workspace/github/scalding/scalding-core/target/scala-2.9.3/classes/com/twitter/scalding/TypedSource9$class.class
// should be com/twitter/scalding/TypedSource9$class.class
// Note that this assumes parent and file are in absolute form if that's what we want
private def add(parent: File, source: File, target: JarOutputStream) {
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

  def arg(inArg: String, value: List[String]) = {
    argsMap += inArg -> value
    this
  }

  def arg(inArg: String, value: String) = {
    argsMap += inArg -> List(value)
    this
  }
/*
//TODO should this be set by them? By us based on the job?
  def mapTasks(num: Int) {
    jobConf.setNumMapTasks(num)
  }

  def reduceTasks(num: Int) {
    jobConf.setNumReduceTasks(num)
  }
*/

  def source(location: String, data: Seq[String]) = typedSource(TypedTsv[String](location), data)

  def source[T](location: String, data: Seq[T]) = typedSource(TypedTsv[T](location), data)
/*
  def source[T](out: Source, data: Seq[T]) = {
    val scaldingSourceWriter = new Job(_: Args) {
      Tsv("dummyInput").read.flatMapTo(0 -> 0) { x:String => data }.write(TypedTsv)
    }
  }
*/
  def typedSource[T](out: TypedSink[T], data: Seq[T]) = {
    sourceWriters.+=){ args: Args =>
      new Job(args: Args) {
        Tsv("dummyInput").read.flatMapTo(0 -> 0) { _:String => data }.write(out)
      }
    })
    this
  }

  //TODO Maybe use TypeTsv here to decode? Can we do that independent of cascading?
  def expect(location: String)(toExpect: Seq[String] => Unit) = {
    expectations.+=((location, toExpect))
    this
  }

  def createDataSources() {
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
      FileUtil.copy(tmpFile, fileSystem, new Path(location), false, jobConf)
      tmpFile.delete()
    }
  }

  private def checkExpectations() {
    expectations.foreach { case (location, toExpect) =>
      val globLocation = location + "/part-*"
      val locations = Option(fileSystem.globStatus(new Path(globLocation)))
        .getOrElse(throw new Exception("No data found for glob: " + globLocation)).map { _.getPath }
      val lines = locations.foldLeft(Seq[String]()) { (cum, hdfsPath) =>
        println("READING FILE: " + hdfsPath) //TODO remove
        val tmpFile = File.createTempFile("hadoop_platform", "job_test")
        tmpFile.deleteOnExit()
        FileUtil.copy(fileSystem, hdfsPath, tmpFile, false,  jobConf)
        val is = new BufferedReader(new FileReader(tmpFile))
        @annotation.tailrec
        def readLines(next: Option[String], lines: Vector[String] = Vector()): Vector[String] =
          next match {
            case Some(line) => readLines(Option(is.readLine()), lines :+ line)
            case None => lines
          }
        val lines = readLines(Option(is.readLine())).toList
        is.close()
        tmpFile.delete()
        cum ++ lines
      }
      toExpect(lines)
    }
  }

  //WE NEED TO SHUT DOWN THE MINICLUSTER
  def run {
    initializeCluster()
    createDataSources()
    runJob(initJob())

    val stat =

    Thread.sleep(10000)

    checkExpectations()
    shutdownCluster()
  }

  private def initJob(): Job =
    // Construct a job.
    cons(Mode.putMode(Hdfs(true, jobConf), new Args(argsMap)))

  @annotation.tailrec
  private final def runJob(job: Job) {
    job.run
    job.clear
    job.next match {
      case Some(nextJob) => runJob(nextJob)
      case None => ()
    }
    //TODO we need to shut down this guy!
  }

  private def shutdownCluster() {
    fileSystem.close()
    dfs.shutdown()
    cluster.shutdown()
    hadoop = None
  }
}
