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

import scala.util.{Failure, Success, Try}

trait ReadPathProvider { self =>
  def readPath(m: Mode): Try[Iterable[String]]

  def filter(fn: (Mode, String) => Boolean): ReadPathProvider = new ReadPathProvider {
    def readPath(m: Mode) = self.readPath(m).map(_.filter(p => fn(m, p)))
  }
  /**
   * This looks at an entire result and if they are good, we continue
   * with the entire set, otherwise we fail
   */
  def validate(fn: (Mode, Iterable[String]) => Try[Unit]): ReadPathProvider = new ReadPathProvider {
    def readPath(m: Mode) =
      for {
        paths <- self.readPath(m)
        _ <- fn(m, paths)
      } yield paths
  }
}

object ReadPathProvider {
  def apply(path: String): ReadPathProvider = new ReadPathProvider {
    def readPath(m: Mode) = Success(List(path))
  }
  def apply(paths: Iterable[String]): ReadPathProvider = new ReadPathProvider {
    def readPath(m: Mode) = Success(paths)
  }
  //TODO: Add daterange via globifier, most-recent, and _SUCCESS file validators.
}

