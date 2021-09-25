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
package com.twitter.scalding

/**
 * Represents a strategy for replicating rows when performing skewed joins.
 */
sealed abstract class SkewReplication {
  val DEFAULT_NUM_REDUCERS = 100

  /**
   * Given the estimated frequencies of a join key in two pipes that we want to skew-join together,
   * this returns the key's replication amount in each pipe.
   *
   * Note: if we switch to a Count-Min sketch, we'll need to change the meaning of these counts
   * from "sampled counts" to "estimates of full counts", and also change how we deal with counts of
   * zero.
   */
  def getReplications(leftCount: Int, rightCount: Int, reducers: Int): (Int, Int)
}

/**
 * See https://github.com/twitter/scalding/pull/229#issuecomment-10773810
 */
final case class SkewReplicationA(replicationFactor: Int = 1) extends SkewReplication {

  override def getReplications(leftCount: Int, rightCount: Int, reducers: Int) = {
    val numReducers = if (reducers <= 0) DEFAULT_NUM_REDUCERS else reducers

    val left = scala.math.min(rightCount * replicationFactor, numReducers)
    val right = scala.math.min(leftCount * replicationFactor, numReducers)

    // Keys with sampled counts of zero still need to be kept, so we set their replication to 1.
    (if (left == 0) 1 else left, if (right == 0) 1 else right)
  }

}

/**
 * See https://github.com/twitter/scalding/pull/229#issuecomment-10792296
 */
final case class SkewReplicationB(maxKeysInMemory: Int = 1E6.toInt, maxReducerOutput: Int = 1E7.toInt)
  extends SkewReplication {

  override def getReplications(leftCount: Int, rightCount: Int, reducers: Int) = {
    val numReducers = if (reducers <= 0) DEFAULT_NUM_REDUCERS else reducers

    val left = scala.math.max(1, rightCount / maxKeysInMemory)
    val right = scala.math.min(numReducers, (leftCount * rightCount) / (maxReducerOutput * left))

    (left, if (right == 0) 1 else right)
  }

}
