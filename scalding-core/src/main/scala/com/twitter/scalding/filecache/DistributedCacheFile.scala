package com.twitter.scalding.filecache

import com.twitter.algebird.MurmurHash128
import com.twitter.scalding._
import java.io.File
import java.net.URI
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.filecache.{ DistributedCache => HDistributedCache }
import org.apache.hadoop.fs.Path

object URIHasher {
  private[this] final val HashFunc = MurmurHash128(1L)

  private[this] def deSign(b: Byte): Int =
    if (b < 0) b + 0xff else b

  def apply(stringUri: String): String =
    apply(new URI(stringUri))

  /**
   * generates hashes of hdfs URIs using algebird's MurmurHash128
   * @param uri the URI to generate a hash for
   * @return a hex-encoded string of the bytes of the 128 bit hash. The results are zero padded on the left, so
   *         this string will always be 32 characters long.
   */
  def apply(uri: URI): String = {
    val (h1, h2) = HashFunc(uri.toASCIIString)
    val bytes = ByteBuffer.allocate(16).putLong(h1).putLong(h2).array()
    bytes.map(deSign).map("%02x".format(_)).reduceLeft(_ + _) // lifted gently from com.twitter.util.U64
  }
}

/**
 * The distributed cache is simply hadoop's method for allowing each node local access to a
 * specific file. The registration of that file must be called with the Configuration of the job,
 * and not when it's on a mapper or reducer. Additionally, a unique name for the node-local access
 * path must be used to prevent collisions in the cluster. This class provides this functionality.
 *
 * In the configuration phase, the file URI is used to construct an UncachedFile instance. The name
 * of the symlink to use on the mappers is only available after calling the add() method, which
 * registers the file and computes the unique symlink name and returns a CachedFile instance.
 * The CachedFile instance is Serializable, it's designed to be assigned to a val and accessed later.
 *
 * The local symlink is available thorugh .file or .path depending on what type you need.
 *
 * example:
 *
 * {{{
 * class YourJob(args: Args) extends Job(args) {
 *   val theCachedFile = DistributedCacheFile("/path/to/your/file.txt")
 *
 *   def somethingThatUsesTheCachedFile() {
 *     doSomethingWith(theCachedFile.path) // or theCachedFile.file
 *   }
 * }
 *
 * example with Execution:
 *
 * {{{
 * object YourExecJob extends ExecutionApp {
 *  override def job =
 *    Execution.withCachedFile("/path/to/your/file.txt") { file =>
 *      doSomething(theCachedFile.path)
 *    }
 * }
 *
 * example with Execution and multiple files:
 *
 * object YourExecJob extends ExecutionApp {
 *  override def job =
 *    Execution.withCachedFile("/path/to/your/one.txt") { one =>
 *      Execution.withCachedFile("/path/to/your/second.txt") { second =>
 *        doSomething(one.path, second.path)
 *      }
 *    }
 * }
 *
 * }}}
 *
 */
object DistributedCacheFile {
  /**
   * Create an object that can be used to register a given URI (representing an hdfs file)
   * that should be added to the DistributedCache.
   *
   * @param uri The fully qualified URI that points to the hdfs file to add
   * @return A CachedFile instance
   */
  def apply(uri: URI)(implicit mode: Mode): CachedFile = {
    val cachedFile = UncachedFile(Right(uri)).cached(mode)

    addCachedFile(cachedFile, mode)

    cachedFile
  }

  def apply(path: String)(implicit mode: Mode): CachedFile = {
    val cachedFile = UncachedFile(Left(path)).cached(mode)

    addCachedFile(cachedFile, mode)

    cachedFile
  }

  private[scalding] def cachedFile(uri: URI, mode: Mode): CachedFile =
    UncachedFile(Right(uri)).cached(mode)

  private[scalding] def cachedFile(path: String, mode: Mode): CachedFile =
    UncachedFile(Left(path)).cached(mode)

  private[scalding] def addCachedFile(cachedFile: CachedFile, mode: Mode): Unit = {
    (cachedFile, mode) match {
      case (hadoopFile: HadoopCachedFile, hadoopMode: HadoopMode) =>
        HDistributedCache.addCacheFile(symlinkedUriFor(hadoopFile.sourceUri), hadoopMode.jobConf)
      case _ =>
    }
  }

  def symlinkNameFor(uri: URI): String = {
    val hexsum = URIHasher(uri)
    val fileName = new File(uri.toString).getName

    Seq(hexsum, fileName).mkString("-")
  }

  def symlinkedUriFor(sourceUri: URI): URI =
    new URI(sourceUri.getScheme, sourceUri.getSchemeSpecificPart, symlinkNameFor(sourceUri))
}

final case class UncachedFile private[scalding] (source: Either[String, URI]) {

  def cached(mode: Mode): CachedFile =
    mode match {
      case Hdfs(_, conf) => addHdfs(conf)
      case HadoopTest(conf, _) => addHdfs(conf)
      case (Local(_) | Test(_)) => addLocal()
      case _ => throw new RuntimeException("unhandled mode: %s".format(mode))
    }

  private[this] def addLocal(): CachedFile = {
    val path =
      source match {
        case Left(strPath) => strPath
        case Right(uri) => uri.getPath
      }

    LocallyCachedFile(path)
  }

  private[this] def addHdfs(conf: Configuration): CachedFile = {
    def makeQualifiedStr(path: String, conf: Configuration): URI =
      makeQualified(new Path(path), conf)

    def makeQualifiedURI(uri: URI, conf: Configuration): URI =
      makeQualified(new Path(uri.toString), conf) // uri.toString because hadoop 0.20.2 doesn't take a URI

    def makeQualified(p: Path, conf: Configuration): URI = {
      val fileSystem = p.getFileSystem(conf)
      p.makeQualified(fileSystem.getUri, fileSystem.getWorkingDirectory).toUri
    }

    val sourceUri =
      source match {
        case Left(strPath) => makeQualifiedStr(strPath, conf)
        case Right(uri) => makeQualifiedURI(uri, conf)
      }

    HadoopCachedFile(sourceUri)
  }
}

sealed abstract class CachedFile {
  /** The path to the cahced file on disk (the symlink registered at configuration time) */
  def path: String

  /** The path to the cached file on disk as a File object. */
  def file: File
}

final case class LocallyCachedFile private[scalding] (sourcePath: String) extends CachedFile {
  def path: String = file.getCanonicalPath
  def file: File = new File(sourcePath).getCanonicalFile
}

final case class HadoopCachedFile private[scalding] (sourceUri: URI) extends CachedFile {

  import DistributedCacheFile._

  def path: String = Seq("./", symlinkNameFor(sourceUri)).mkString("")
  def file: File = new File(path)
}
