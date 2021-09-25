//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package com.twitter.scalding
package typed

import scala.collection.JavaConverters._

import cascading.tap.partition.Partition
import cascading.tuple.{ Fields, TupleEntry }

/**
 * Creates a partition using the given template string.
 *
 * The template string needs to have %s as placeholder for a given field.
 */
case class TemplatePartition(partitionFields: Fields, template: String) extends Partition {
  assert(
    partitionFields.size == "%s".r.findAllIn(template).length,
    "Number of partition fields %s does not correspond to template (%s)".format(partitionFields, template))

  /** Regex pattern created from the template to extract the partition values from a path.*/
  lazy val pattern = template.replaceAll("%s", "(.*)").r.pattern

  /** Returns the path depth. In this case the number of partition fields. */
  override def getPathDepth(): Int = partitionFields.size

  /** Returns the partition fields. */
  override def getPartitionFields(): Fields = partitionFields

  /**
   * Converts the given partition string to field values and populates the supplied tuple entry
   * with it.
   */
  override def toTuple(partition: String, tupleEntry: TupleEntry): Unit = {
    val m = pattern.matcher(partition)
    m.matches
    val parts: Array[Object] = (1 to partitionFields.size).map(i => m.group(i)).toArray
    tupleEntry.setCanonicalValues(parts)
  }

  /**
   * Given the specified tuple entry fill in the supplied template entry to create the partition
   * path.
   */
  override def toPartition(tupleEntry: TupleEntry): String = {
    val fields = tupleEntry.asIterableOf(classOf[String]).asScala.toList
    template.format(fields: _*)
  }
}
