/*
Copyright 2015 Twitter, Inc.

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

package com.twitter.scalding.serialization

import java.io.InputStream
import java.util.Comparator
import cascading.flow.FlowDef
import cascading.tuple.{ Fields, Hasher => CHasher, StreamComparator }

import scala.util.{ Failure, Success, Try }
/**
 * This is the type that should be fed to cascading to enable binary comparators
 */
class CascadingBinaryComparator[T](ob: OrderedSerialization[T]) extends Comparator[T]
  with StreamComparator[InputStream]
  with CHasher[T]
  with Serializable {

  override def compare(a: T, b: T) = ob.compare(a, b)
  override def hashCode(t: T): Int = ob.hash(t)
  override def compare(a: InputStream, b: InputStream) =
    ob.compareBinary(a, b).unsafeToInt
}

object CascadingBinaryComparator {
  /**
   * This method will walk the flowDef and make sure all the
   * groupBy/cogroups are using a CascadingBinaryComparator
   */
  def checkForOrderedSerialization(fd: FlowDef): Try[Unit] = {
    // Get the asScala enrichments locally?
    import collection.JavaConverters._
    import cascading.pipe._
    import com.twitter.scalding.RichPipe

    // all successes or empty returns success
    def reduce(it: TraversableOnce[Try[Unit]]): Try[Unit] =
      it.find(_.isFailure).getOrElse(Success(()))

    def check(s: Splice): Try[Unit] = {
      val m = s.getKeySelectors.asScala
      if (m.isEmpty) Failure(new Exception(s"Splice must have KeySelectors: $s"))
      else {
        reduce(m.map {
          case (pipename, fields) =>
            Try {
              if (fields.getComparators()(0).isInstanceOf[com.twitter.scalding.serialization.CascadingBinaryComparator[_]])
                ()
              else sys.error(s"pipe: $s, fields: $fields, comparators: ${fields.getComparators.toList}")
            }
        })
      }
    }

    val allPipes: Set[Pipe] = fd.getTails.asScala.map(p => RichPipe(p).upstreamPipes).flatten.toSet
    reduce(allPipes.iterator.map {
      case gb: GroupBy => check(gb)
      case cg: CoGroup => check(cg)
      case _ => Success(())
    })
  }
}
