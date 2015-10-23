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

import cascading.flow.Flow
import cascading.flow.planner.BaseFlowStep
import cascading.tuple.{ Hasher => CHasher, StreamComparator }
import com.twitter.scalding.ExecutionContext.getDesc
import java.io.InputStream
import java.util.Comparator
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
  private[scalding] def checkForOrderedSerialization[T](flow: Flow[T]): Try[Unit] = {
    import collection.JavaConverters._
    import cascading.pipe._
    import com.twitter.scalding.RichPipe

    // all successes or empty returns success
    def reduce(it: TraversableOnce[Try[Unit]]): Try[Unit] =
      it.find(_.isFailure).getOrElse(Success(()))

    def check(s: Splice): Try[Unit] = {
      val m = s.getKeySelectors.asScala
      val sortingSelectors = s.getSortingSelectors.asScala

      def error(s: String): Try[Unit] =
        Failure(new RuntimeException("Cannot verify OrderedSerialization: " + s))

      if (m.isEmpty) error(s"Splice must have KeySelectors: $s")
      else {
        reduce(m.map {
          case (pipename, fields) =>
            /*
             * Scalding typed-API ALWAYS puts the key into field position 0.
             * If OrderedSerialization is enabled, this must be a CascadingBinaryComparator
             */
            if (fields.getComparators()(0).isInstanceOf[CascadingBinaryComparator[_]])
              Success(())
            else error(s"pipe: $s, fields: $fields, comparators: ${fields.getComparators.toList}")
        })
      }
    }

    def getDescriptionsForMissingOrdSer[U](bfs: BaseFlowStep[U]): Option[String] =
      // does this job have any Splices without OrderedSerialization:
      if (bfs.getElementGraph.vertexSet.asScala.exists {
        case gb: GroupBy => check(gb).isFailure
        case cg: CoGroup => check(cg).isFailure
        case _ => false // only do sorting in groupBy/cogroupBy
      }) {
        Some(getDesc(bfs).mkString(", "))
      } else None

    // Get all the steps that have missing OrderedSerializations
    val missing = flow.getFlowSteps.asScala
      .map { case bfs: BaseFlowStep[_] => getDescriptionsForMissingOrdSer(bfs) }
      .collect { case Some(desc) => desc }

    if (missing.isEmpty) Success(())
    else {
      val badSteps = missing.size
      val msg = missing.zipWithIndex.map { case (msg, idx) => s"<step$idx>$msg</step$idx>" }.mkString
      sys.error(s"There are $badSteps missing OrderedSerializations: $msg")
    }
  }
}
