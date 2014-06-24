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

import cascading.tap._
import cascading.scheme._
import cascading.pipe._
import cascading.pipe.assembly._
import cascading.pipe.joiner._
import cascading.flow._
import cascading.operation._
import cascading.operation.aggregator._
import cascading.operation.filter._
import cascading.tuple._
import cascading.cascade._

import scala.util.Random
import scala.collection.JavaConverters._

object JoinAlgorithms extends java.io.Serializable {
  // seed is ascii codes for "scalding" combined as affine maps.
  val Seed: Long = (((115L * 99 + 97) * 108 + 100) * 105 + 110) * 103
}

/*
 * Keeps all the logic related to RichPipe joins.
 *
 */
trait JoinAlgorithms {
  import Dsl._
  import RichPipe.assignName
  import JoinAlgorithms.Seed

  def pipe: Pipe

  /**
   * This method is used internally to implement all joins.
   * You can use this directly if you want to implement something like a star join,
   * e.g., when joining a single pipe to multiple other pipes. Make sure that you call this method
   * on the larger pipe to make the grouping as efficient as possible.
   *
   * If you are only joining two pipes, then you are better off
   * using joinWithSmaller/joinWithLarger/joinWithTiny/leftJoinWithTiny.
   *
   */
  def coGroupBy(f: Fields, j: JoinMode = InnerJoinMode)(builder: CoGroupBuilder => GroupBuilder): Pipe = {
    builder(new CoGroupBuilder(f, j)).schedule(pipe.getName, pipe)
  }

  /**
   * == WARNING ==
   * Doing a cross product with even a moderate sized pipe can
   * create ENORMOUS output.  The use-case here is attaching a constant (e.g.
   * a number or a dictionary or set) to each row in another pipe.
   * A common use-case comes from a groupAll and reduction to one row,
   * then you want to send the results back out to every element in a pipe
   *
   * This uses joinWithTiny, so tiny pipe is replicated to all Mappers.  If it
   * is large, this will blow up.  Get it: be foolish here and LOSE IT ALL!
   *
   * Use at your own risk.
   */
  def crossWithTiny(tiny: Pipe) = {
    val tinyJoin = tiny.map(() -> '__joinTiny__) { (u: Unit) => 1 }
    pipe.map(() -> '__joinBig__) { (u: Unit) => 1 }
      .joinWithTiny('__joinBig__ -> '__joinTiny__, tinyJoin)
      .discard('__joinBig__, '__joinTiny__)
  }
  /**
   * Does a cross-product by doing a blockJoin.
   * Useful when doing a large cross, if your cluster can take it.
   * Prefer crossWithTiny
   */
  def crossWithSmaller(p: Pipe, replication: Int = 20) = {
    val smallJoin = p.map(() -> '__joinSmall__) { (u: Unit) => 1 }
    pipe.map(() -> '__joinBig__) { (u: Unit) => 1 }
      .blockJoinWithSmaller('__joinBig__ -> '__joinSmall__, smallJoin, rightReplication = replication)
      .discard('__joinBig__, '__joinSmall__)
  }

  /**
   * Rename the collisions and return the pipe and the new names,
   * and the fields to discard
   */
  private def renameCollidingFields(p: Pipe, fields: Fields,
    collisions: Set[Comparable[_]]): (Pipe, Fields, Fields) = {
    // Here is how we rename colliding fields
    def rename(f: Comparable[_]): String = "__temp_join_" + f.toString

    // convert to list, so we are explicit that ordering is fixed below:
    val renaming = collisions.toList
    val orig = new Fields(renaming: _*)
    val temp = new Fields(renaming.map { rename }: _*)
    // Now construct the new join keys, where we check for a rename
    // otherwise use the original key:
    val newJoinKeys = new Fields(asList(fields)
      .map { fname =>
        // If we renamed, get the rename, else just use the field
        if (collisions(fname)) {
          rename(fname)
        } else fname
      }: _*)
    val renamedPipe = p.rename(orig -> temp)
    (renamedPipe, newJoinKeys, temp)
  }

  /**
   * Flip between LeftJoin to RightJoin
   */
  private def flipJoiner(j: Joiner) = {
    j match {
      case outer: OuterJoin => outer
      case inner: InnerJoin => inner
      case left: LeftJoin => new RightJoin
      case right: RightJoin => new LeftJoin
      case other => throw new InvalidJoinModeException("cannot use joiner " + other +
        " since it cannot be flipped safely")
    }
  }

  def joinerToJoinModes(j: Joiner) = {
    j match {
      case i: InnerJoin => (InnerJoinMode, InnerJoinMode)
      case l: LeftJoin => (InnerJoinMode, OuterJoinMode)
      case r: RightJoin => (OuterJoinMode, InnerJoinMode)
      case o: OuterJoin => (OuterJoinMode, OuterJoinMode)
      case _ => throw new InvalidJoinModeException("cannot convert joiner to joiner modes")
    }
  }

  /**
   * Joins the first set of keys in the first pipe to the second set of keys in the second pipe.
   * All keys must be unique UNLESS it is an inner join, then duplicated join keys are allowed, but
   * the second copy is deleted (as cascading does not allow duplicated field names).
   *
   * Smaller here means that the values/key is smaller than the left.
   *
   * Avoid going crazy adding more explicit join modes.  Instead do for some other join
   * mode with a larger pipe:
   *
   * {{{
   * .then { pipe => other.
   *           joinWithSmaller(('other1, 'other2)->('this1, 'this2), pipe, new FancyJoin)
   *       }
   * }}}
   */
  def joinWithSmaller(fs: (Fields, Fields), that: Pipe, joiner: Joiner = new InnerJoin, reducers: Int = -1) = {
    // If we are not doing an inner join, the join fields must be disjoint:
    val joiners = joinerToJoinModes(joiner)
    val intersection = asSet(fs._1).intersect(asSet(fs._2))
    if (intersection.isEmpty) {
      // Common case: no intersection in names: just CoGroup, which duplicates the grouping fields:
      pipe.coGroupBy(fs._1, joiners._1) {
        _.coGroup(fs._2, that, joiners._2)
          .reducers(reducers)
      }
    } else if (joiners._1 == InnerJoinMode && joiners._2 == InnerJoinMode) {
      /*
       * Since it is an inner join, we only output if the key is present an equal in both sides.
       * For this (common) case, it doesn't matter if we drop one of the matching grouping fields.
       * So, we rename the right hand side to temporary names, then discard them after the operation
       */
      val (renamedThat, newJoinFields, temp) = renameCollidingFields(that, fs._2, intersection)
      pipe.coGroupBy(fs._1, joiners._1) {
        _.coGroup(newJoinFields, renamedThat, joiners._2)
          .reducers(reducers)
      }.discard(temp)
    } else {
      throw new IllegalArgumentException("join keys must be disjoint unless you are doing an InnerJoin.  Found: " +
        fs.toString + ", which overlap with: " + intersection.toString)
    }
  }

  /**
   * same as reversing the order on joinWithSmaller
   */
  def joinWithLarger(fs: (Fields, Fields), that: Pipe, joiner: Joiner = new InnerJoin, reducers: Int = -1) = {
    that.joinWithSmaller((fs._2, fs._1), pipe, flipJoiner(joiner), reducers)
  }

  /**
   * This is joinWithSmaller with joiner parameter fixed to LeftJoin. If the item is absent on the right put null for the keys and values
   */
  def leftJoinWithSmaller(fs: (Fields, Fields), that: Pipe, reducers: Int = -1) = {
    joinWithSmaller(fs, that, new LeftJoin, reducers)
  }

  /**
   * This is joinWithLarger with joiner parameter fixed to LeftJoin. If the item is absent on the right put null for the keys and values
   */
  def leftJoinWithLarger(fs: (Fields, Fields), that: Pipe, reducers: Int = -1) = {
    joinWithLarger(fs, that, new LeftJoin, reducers)
  }

  /**
   * This does an assymmetric join, using cascading's "HashJoin".  This only runs through
   * this pipe once, and keeps the right hand side pipe in memory (but is spillable).
   *
   * Choose this when Left > max(mappers,reducers) * Right, or when the left side is three
   * orders of magnitude larger.
   *
   * joins the first set of keys in the first pipe to the second set of keys in the second pipe.
   * Duplicated join keys are allowed, but
   * the second copy is deleted (as cascading does not allow duplicated field names).
   *
   *
   * == Warning ==
   * This does not work with outer joins, or right joins, only inner and
   * left join versions are given.
   */
  def joinWithTiny(fs: (Fields, Fields), that: Pipe) = {
    val intersection = asSet(fs._1).intersect(asSet(fs._2))
    if (intersection.isEmpty) {
      new HashJoin(assignName(pipe), fs._1, assignName(that), fs._2, new InnerJoin)
    } else {
      val (renamedThat, newJoinFields, temp) = renameCollidingFields(that, fs._2, intersection)
      (new HashJoin(assignName(pipe), fs._1, assignName(renamedThat), newJoinFields, new InnerJoin))
        .discard(temp)
    }
  }

  def leftJoinWithTiny(fs: (Fields, Fields), that: Pipe) = {
    //Rename these pipes to avoid cascading name conflicts
    new HashJoin(assignName(pipe), fs._1, assignName(that), fs._2, new LeftJoin)
  }

  /**
   * Performs a block join, otherwise known as a replicate fragment join (RF join).
   * The input params leftReplication and rightReplication control the replication of the left and right
   * pipes respectively.
   *
   * This is useful in cases where the data has extreme skew. A symptom of this is that we may see a job stuck for
   * a very long time on a small number of reducers.
   *
   * A block join is way to get around this: we add a random integer field and a replica field
   * to every tuple in the left and right pipes. We then join on the original keys and
   * on these new dummy fields. These dummy fields make it less likely that the skewed keys will
   * be hashed to the same reducer.
   *
   * The final data size is right * rightReplication + left * leftReplication
   * but because of the fragmentation, we are guaranteed the same number of hits as the original join.
   *
   * If the right pipe is really small then you are probably better off with a joinWithTiny. If however
   * the right pipe is medium sized, then you are better off with a blockJoinWithSmaller, and a good rule
   * of thumb is to set rightReplication = left.size / right.size and leftReplication = 1
   *
   * Finally, if both pipes are of similar size, e.g. in case of a self join with a high data skew,
   * then it makes sense to set leftReplication and rightReplication to be approximately equal.
   *
   * == Note ==
   * You can only use an InnerJoin or a LeftJoin with a leftReplication of 1
   * (or a RightJoin with a rightReplication of 1) when doing a blockJoin.
   */
  def blockJoinWithSmaller(fs: (Fields, Fields),
    otherPipe: Pipe, rightReplication: Int = 1, leftReplication: Int = 1,
    joiner: Joiner = new InnerJoin, reducers: Int = -1): Pipe = {

    assert(rightReplication > 0, "Must specify a positive number for the right replication in block join")
    assert(leftReplication > 0, "Must specify a positive number for the left replication in block join")
    assertValidJoinMode(joiner, leftReplication, rightReplication)

    // These are the new dummy fields used in the skew join
    val leftFields = new Fields("__LEFT_I__", "__LEFT_J__")
    val rightFields = new Fields("__RIGHT_I__", "__RIGHT_J__")

    // Add the new dummy replication fields
    val newLeft = addReplicationFields(pipe, leftFields, leftReplication, rightReplication)
    val newRight = addReplicationFields(otherPipe, rightFields, rightReplication, leftReplication, swap = true)

    val leftJoinFields = Fields.join(fs._1, leftFields)
    val rightJoinFields = Fields.join(fs._2, rightFields)

    newLeft
      .joinWithSmaller((leftJoinFields, rightJoinFields), newRight, joiner, reducers)
      .discard(leftFields)
      .discard(rightFields)
  }

  /**
   * Adds one random field and one replica field.
   */
  private def addReplicationFields(p: Pipe, f: Fields,
    replication: Int, otherReplication: Int, swap: Boolean = false): Pipe = {
    /**
     * We need to seed exactly once and capture that seed. If we let
     * each task create a seed, a restart will change the computation,
     * and this could result in subtle bugs.
     */
    p.using(new Random(Seed) with Stateful).flatMap(() -> f) { (rand: Random, _: Unit) =>
      val rfs = getReplicationFields(rand, replication, otherReplication)
      if (swap) rfs.map { case (i, j) => (j, i) } else rfs
    }
  }

  /**
   * Returns a list of the dummy replication fields used to replicate groups in skewed joins.
   *
   * For example, suppose you have two pipes P1 and P2. While performing a skewed join for a particular
   * key K, you want to replicate every row in P1 with this key 3 times, and every row in P2 with this
   * key 5 times.
   *
   * Then:
   *
   * - For the P1 replication, the first element of each tuple is the same random integer between 0 and 4,
   *   and the second element of each tuple is the index of the replication (between 0 and 2). This first
   *   random element guarantees that we will match exactly one random row in P2 with the same key.
   * - Similarly for the P2 replication.
   *
   * Examples:
   *
   *   getReplicationFields(3, 5)
   *     => List( (1, 0), (1, 1), (1, 2) )
   *
   *   getReplicationFields(5, 3)
   *     => List( (2, 0), (2, 1), (2, 2), (2, 3), (2, 4) )
   */
  private def getReplicationFields(r: Random, replication: Int, otherReplication: Int): IndexedSeq[(Int, Int)] = {
    assert(replication >= 1 && otherReplication >= 1, "Replication counts must be >= 1")

    val rand = r.nextInt(otherReplication)
    (0 until replication).map { rep => (rand, rep) }
  }

  private def assertValidJoinMode(joiner: Joiner, left: Int, right: Int): Unit = {
    (joiner, left, right) match {
      case (i: InnerJoin, _, _) => ()
      case (k: LeftJoin, 1, _) => ()
      case (m: RightJoin, _, 1) => ()
      case (j, l, r) =>
        throw new InvalidJoinModeException(
          "you cannot use joiner " + j + " with left replication " + l + " and right replication " + r)
    }
  }

  /**
   * Performs a skewed join, which is useful when the data has extreme skew.
   *
   * For example, imagine joining a pipe of Twitter's follow graph against a pipe of user genders,
   * in order to find the gender distribution of the accounts every Twitter user follows. Since celebrities
   * (e.g., Justin Bieber and Lady Gaga) have a much larger follower base than other users, and (under
   * a standard join algorithm) all their followers get sent to the same reducer, the job will likely be
   * stuck on a few reducers for a long time. A skewed join attempts to alleviate this problem.
   *
   * This works as follows:
   *
   * 1. First, we sample from the left and right pipes with some small probability, in order to determine
   *    approximately how often each join key appears in each pipe.
   * 2. We use these estimated counts to replicate the join keys, according to the given replication strategy.
   * 3. Finally, we join the replicated pipes together.
   *
   * @param sampleRate This controls how often we sample from the left and right pipes when estimating key counts.
   * @param replicator Algorithm for determining how much to replicate a join key in the left and right pipes.
   *
   * Note: since we do not set the replication counts, only inner joins are allowed. (Otherwise, replicated
   * rows would stay replicated when there is no counterpart in the other pipe.)
   */
  def skewJoinWithSmaller(fs: (Fields, Fields), otherPipe: Pipe,
    sampleRate: Double = 0.001, reducers: Int = -1,
    replicator: SkewReplication = SkewReplicationA()): Pipe = {

    assert(sampleRate > 0 && sampleRate < 1, "Sampling rate for skew joins must lie strictly between 0 and 1")

    val intersection = asSet(fs._1).intersect(asSet(fs._2))

    // Resolve colliding fields
    val (rightPipe, rightResolvedJoinFields, dupeFields) =
      if (intersection.isEmpty)
        (otherPipe, fs._2, Fields.NONE)
      else // For now, we are assuming an inner join.
        renameCollidingFields(otherPipe, fs._2, intersection)

    // 1. First, get an approximate count of the left join keys and the right join keys, so that we
    // know how much to replicate.
    // TODO: try replacing this with a Count-Min sketch.
    val leftSampledCountField = "__LEFT_SAMPLED_COUNT__"
    val rightSampledCountField = "__RIGHT_SAMPLED_COUNT__"
    val sampledCountFields = new Fields(leftSampledCountField, rightSampledCountField)

    /**
     * We need to seed exactly once and capture that seed. If we let
     * each task create a seed, a restart will change the computation,
     * and this could result in subtle bugs.
     */
    val sampledLeft = pipe.sample(sampleRate, Seed)
      .groupBy(fs._1) { _.size(leftSampledCountField) }
    val sampledRight = rightPipe.sample(sampleRate, Seed)
      .groupBy(rightResolvedJoinFields) { _.size(rightSampledCountField) }
    val sampledCounts = sampledLeft.joinWithSmaller(fs._1 -> rightResolvedJoinFields, sampledRight, joiner = new OuterJoin)
      .project(Fields.join(fs._1, rightResolvedJoinFields, sampledCountFields))

    // 2. Now replicate each group of join keys in the left and right pipes, according to the sampled counts
    // from the previous step.
    val leftReplicationFields = new Fields("__LEFT_RAND__", "__LEFT_REP__")
    val rightReplicationFields = new Fields("__RIGHT_REP__", "__RIGHT_RAND__")

    val replicatedLeft = skewReplicate(pipe, sampledCounts, fs._1, sampledCountFields, leftReplicationFields,
      replicator, reducers)
    val replicatedRight = skewReplicate(rightPipe, sampledCounts, rightResolvedJoinFields, sampledCountFields, rightReplicationFields,
      replicator, reducers, true)

    // 3. Finally, join the replicated pipes together.
    val leftJoinFields = Fields.join(fs._1, leftReplicationFields)
    val rightJoinFields = Fields.join(rightResolvedJoinFields, rightReplicationFields)

    val joinedPipe =
      replicatedLeft
        .joinWithSmaller(leftJoinFields -> rightJoinFields, replicatedRight, joiner = new InnerJoin, reducers)
        .discard(leftReplicationFields)
        .discard(rightReplicationFields)

    if (intersection.isEmpty) joinedPipe
    else joinedPipe.discard(dupeFields)
  }

  def skewJoinWithLarger(fs: (Fields, Fields), otherPipe: Pipe,
    sampleRate: Double = 0.001, reducers: Int = -1,
    replicator: SkewReplication = SkewReplicationA()): Pipe = {
    otherPipe.skewJoinWithSmaller((fs._2, fs._1), pipe, sampleRate, reducers, replicator)
  }

  /**
   * Helper method for performing skewed joins. This replicates the rows in {pipe} according
   * to the estimated counts in {sampledCounts}.
   *
   * @param pipe The pipe to be replicated.
   * @param sampledCounts A pipe containing, for each key, the estimated counts of how often
   *                      this key appeared in the samples of the original left and right pipes.
   * @param replicator Strategy for how the pipe is replicated.
   * @param isPipeOnRight Set to true when replicating the right pipe.
   */
  private def skewReplicate(pipe: Pipe, sampledCounts: Pipe, joinFields: Fields,
    countFields: Fields, replicationFields: Fields,
    replicator: SkewReplication,
    numReducers: Int = -1, isPipeOnRight: Boolean = false) = {

    // Rename the fields to prepare for the leftJoin below.
    val renamedFields = joinFields.iterator.asScala.toList.map { field => "__RENAMED_" + field + "__" }
    val renamedSampledCounts = sampledCounts.rename(joinFields -> renamedFields)
      .project(Fields.join(renamedFields, countFields))

    /**
     * We need to seed exactly once and capture that seed. If we let
     * each task create a seed, a restart will change the computation,
     * and this could result in subtle bugs.
     */
    pipe
      // Join the pipe against the sampled counts, so that we know approximately how often each
      // join key appears.
      .leftJoinWithTiny(joinFields -> renamedFields, renamedSampledCounts)
      .using(new Random(Seed) with Stateful)
      .flatMap(countFields -> replicationFields) { (rand: Random, counts: (Int, Int)) =>
        val (leftRep, rightRep) = replicator.getReplications(counts._1, counts._2, numReducers)

        val (rep, otherRep) = if (isPipeOnRight) (rightRep, leftRep) else (leftRep, rightRep)
        val rfs = getReplicationFields(rand, rep, otherRep)
        if (isPipeOnRight) rfs.map(_.swap) else rfs
      }
      .discard(renamedFields)
      .discard(countFields)
  }
}

class InvalidJoinModeException(args: String) extends Exception(args)
