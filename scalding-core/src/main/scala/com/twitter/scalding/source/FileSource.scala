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
package com.twitter.scalding.source

import org.apache.hadoop.fs.{ FileStatus, PathFilter, Path }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd._

object HiddenFileFilter extends PathFilter {
  def accept(p: Path) = {
    val name = p.getName
    !name.startsWith("_") && !name.startsWith(".")
  }
}

object SuccessFileFilter extends PathFilter {
  def accept(p: Path) = { p.getName == "_SUCCESS" }
}

object AcceptAllPathFilter extends PathFilter {
  def accept(p: Path) = true
}

object FileSource {

  def glob(glob: String, job: Job, filter: PathFilter = AcceptAllPathFilter): Iterable[FileStatus] = {
    val path = new Path(glob)
    Option(path.getFileSystem(job.getConfiguration).globStatus(path, filter)).map {
      _.toIterable // convert java Array to scala Iterable
    } getOrElse {
      Iterable.empty
    }
  }

  /**
   * @return whether globPath contains non hidden files
   */
  def globHasNonHiddenPaths(globPath: String, job: Job): Boolean = {
    glob(globPath, job, HiddenFileFilter).nonEmpty
  }

  /**
   * @return whether globPath contains a _SUCCESS file
   */
  def globHasSuccessFile(globPath: String, job: Job): Boolean = {
    glob(globPath, job, SuccessFileFilter).nonEmpty
  }

}

/**
 * This is a base class for File-based sources
 */
abstract class FileSource[T, M <: RDD[T]] extends Source[T, M] {

  /**
   * Determines if a path is 'valid' for this source. In strict mode all paths must be valid.
   * In non-strict mode, all invalid paths will be filtered out.
   *
   * Subclasses can override this to validate paths.
   *
   * The default implementation is a quick sanity check to look for missing or empty directories.
   * It is necessary but not sufficient -- there are cases where this will return true but there is
   * in fact missing data.
   *
   */
  protected def pathIsGood(p: String, job: Job) = FileSource.globHasNonHiddenPaths(p, job)

  def hdfsPaths: Iterable[String]

  def goodPaths(job: Job): Iterable[String] = {
    hdfsPaths.filter(pathIsGood(_, job))
  }

  // By default, we write to the LAST path returned by hdfsPaths
  def hdfsWritePath = hdfsPaths.last

  def jobWithInputPaths: Job = {
    val job = Job.getInstance

    goodPaths(job).foreach { p =>
      FileInputFormat.addInputPath(job, new Path(p))
    }

    FileInputFormat.setMaxInputSplitSize(job, 268435456L)
    FileInputFormat.setMinInputSplitSize(job, 25456L)
    job
  }

  // This is only called when Mode.sourceStrictness is true
  protected def hdfsReadPathsAreGood(job: Job) = {
    hdfsPaths.forall { pathIsGood(_, job) }
  }
}

/**
 * Ensures that a _SUCCESS file is present in the Source path, which must be a glob,
 * as well as the requirements of [[com.twitter.spark_internal.source.FileSource.pathIsGood]]
 */
trait SuccessFileSource[T, M <: RDD[T]] extends FileSource[T, M] {
  override protected def pathIsGood(p: String, job: Job) = {
    FileSource.globHasNonHiddenPaths(p, job) && FileSource.globHasSuccessFile(p, job)
  }
}

abstract class FixedPathSource[T, M <: RDD[T]](path: String*) extends FileSource[T, M] {
  def localPath = { assert(path.size == 1, "Cannot use multiple input files on local mode"); path(0) }
  def hdfsPaths = path.toList
  override def toString = getClass.getName + path
  override def hashCode = toString.hashCode
  override def equals(that: Any): Boolean = (that != null) && (that.toString == toString)
}
