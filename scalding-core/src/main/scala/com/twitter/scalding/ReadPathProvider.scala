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

import com.twitter.algebird.Monoid
import java.io.Serializable
import java.util.TimeZone
import scala.util.{ Failure, Success, Try }

trait PathValidator extends Serializable { self =>
  def validate(m: Mode, paths: Iterable[String]): Try[Unit]
  /**
   * Use this to combine two path validators into one. This
   * provides a Monoid for PathValidator
   */
  def ++(that: PathValidator): PathValidator = new PathValidator {
    def validate(m: Mode, p: Iterable[String]) = for {
      _ <- self.validate(m, p)
      _ <- that.validate(m, p)
    } yield ()
  }
}

object PathValidator {
  private case class FnPathValidator(fn: (Mode, Iterable[String]) => Try[Unit]) extends PathValidator {
    def validate(m: Mode, paths: Iterable[String]) = fn(m, paths)
  }
  def apply(fn: (Mode, Iterable[String]) => Try[Unit]): PathValidator = FnPathValidator(fn)

  /**
   * This always succeeds
   */
  def empty: PathValidator = apply { (m: Mode, p: Iterable[String]) => Success(()) }

  implicit def monoid: Monoid[PathValidator] = Monoid.from(empty)(_ ++ _)

  /**
   * This prevents the job from running if any of the paths are empty.
   */
  def hasAllNonHiddenPaths: PathValidator = apply { (m: Mode, p: Iterable[String]) =>
    m match {
      case Hdfs(_, conf) =>
        val badPaths = p.filterNot { path => FileSource.globHasNonHiddenPaths(path, conf) }
        if (badPaths.isEmpty) Success(())
        else Failure(new InvalidSourceException("These paths are empty: " + badPaths))
      case _ => Success(())
    }
  }
  /**
   * This prevents the job from running if all paths are empty.
   */
  def hasSomeNonHiddenPaths: PathValidator = apply { (m: Mode, p: Iterable[String]) =>
    m match {
      case Hdfs(_, conf) =>
        val goodPaths = p.filter { path => FileSource.globHasNonHiddenPaths(path, conf) }
        if (goodPaths.nonEmpty) Success(())
        else Failure(new InvalidSourceException("All paths are empty: " + p))
      case _ => Success(())
    }
  }
  /**
   * This prevents the job from running if any paths lack _SUCCESS files
   */
  def hasSuccessFiles: PathValidator = apply { (m: Mode, p: Iterable[String]) =>
    m match {
      case Hdfs(_, conf) =>
        val badPaths = p.filterNot { path => FileSource.globHasSuccessFile(path, conf) }
        if (badPaths.isEmpty) Success(())
        else Failure(new InvalidSourceException("These paths are empty: " + badPaths))
      case _ => Success(())
    }
  }
}

/**
 * This trait provides paths to read, which can
 * be different based on the Mode
 */
trait ReadPathProvider extends Serializable { self =>
  def readPath(m: Mode): Try[Iterable[String]]

  def filter(fn: (Mode, String) => Boolean): ReadPathProvider = new ReadPathProvider {
    def readPath(m: Mode) = self.readPath(m).map(_.filter(p => fn(m, p)))
  }
  /**
   * This looks at an entire result and if they are good, we continue
   * with the entire set, otherwise we fail
   */
  def validate(v: PathValidator): ReadPathProvider = new ReadPathProvider {
    def readPath(m: Mode) =
      for {
        paths <- self.readPath(m)
        _ <- v.validate(m, paths)
      } yield paths
  }

  /**
   * Concatenate two path readers if both are valid
   * There is a Monoid on ReadPathProviders:
   */
  def ++(that: ReadPathProvider): ReadPathProvider = new ReadPathProvider {
    def readPath(m: Mode) = for {
      a <- self.readPath(m)
      b <- that.readPath(m)
    } yield (a ++ b)
  }
}

object ReadPathProvider extends Serializable {
  def apply(path: String): ReadPathProvider = new ReadPathProvider {
    def readPath(m: Mode) = Success(List(path))
  }
  def apply(paths: Iterable[String]): ReadPathProvider = new ReadPathProvider {
    def readPath(m: Mode) = Success(paths)
  }
  /**
   * Uses a pattern including "%1$tH" for Hours, "%1$td" for Days,
   * "%1$tm" for Months and "%1$tY" for Years
   * Note, you need to be careful with validators and globs, they will expand
   * to match what is on disk, not what you want to be there.
   * See validatedDateRange for a version that handles this by enumerating
   * locally before checking the validator.
   */
  def dateRange(pattern: String, dr: DateRange, tz: TimeZone): ReadPathProvider =
    new ReadPathProvider {
      def readPath(m: Mode) = Try(Globifier(pattern)(tz).globify(dr))
    }

  /**
   * Here is an empty reader
   */
  val empty: ReadPathProvider = apply(Nil)

  /**
   * This will not do any globbing of the dates, but will make one entry
   * for every item in the final path.
   */
  def enumeratedDateRange(pattern: String, dateRange: DateRange, tz: TimeZone): ReadPathProvider =
    new ReadPathProvider {
      def readPath(m: Mode) =
        TimePathedSource.stepSize(pattern, tz) match {
          case Some(dur) =>
            // This method is exhaustive, but may be too expensive for Cascading's JobConf writing.
            Success(dateRange.each(dur)
              .map { dr => TimePathedSource.toPath(pattern, dr.start, tz) })
          case None => Failure(new InvalidSourceException("Invalid time path: " + pattern))
        }
    }

  /**
   * like dateRange, but with path validation. The validation enumerates all paths and checks
   * all of them.
   */
  def validatedDateRange(pattern: String, dr: DateRange, tz: TimeZone, v: PathValidator): ReadPathProvider = {
    val efficient = dateRange(pattern, dr, tz)
    val exhaustive = enumeratedDateRange(pattern, dr, tz)
    val exvalidator = PathValidator { (m: Mode, _: Iterable[String]) =>
      // ignore the paths you get here, and check the exhaustive paths:
      for {
        ps <- exhaustive.readPath(m)
        _ <- v.validate(m, ps)
      } yield ()
    }
    efficient.validate(exvalidator)
  }

  implicit def monoid: Monoid[ReadPathProvider] = Monoid.from(empty)(_ ++ _)
}

trait WritePathProvider extends Serializable { self =>
  def writePath(m: Mode): Try[String]
  def validate(v: PathValidator): WritePathProvider = new WritePathProvider {
    def writePath(m: Mode) = for {
      path <- self.writePath(m)
      _ <- v.validate(m, List(path))
    } yield path // we only get here if fn is not a failure
  }
}

object WritePathProvider extends Serializable {
  def apply(path: String): WritePathProvider = new WritePathProvider {
    def writePath(m: Mode) = Success(path)
  }
}
