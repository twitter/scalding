/*
Copyright 2012 Twitter, Inc.

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

import cascading.pipe._
import cascading.pipe.assembly._
import cascading.operation._
import cascading.tuple.Fields
import cascading.tuple.TupleEntry

import scala.{ Range => ScalaRange }

/**
 * This controls the sequence of reductions that happen inside a
 * particular grouping operation.  Not all elements can be combined,
 * for instance, a scanLeft/foldLeft generally requires a sorting
 * but such sorts are (at least for now) incompatible with doing a combine
 * which includes some map-side reductions.
 */
class GroupBuilder(val groupFields: Fields) extends FoldOperations[GroupBuilder] with StreamOperations[GroupBuilder] {
  // We need the implicit conversions from symbols to Fields
  import Dsl._

  /**
   * Holds the "reducers/combiners", the things that we can do paritially map-side.
   */
  private var reds: Option[List[AggregateBy]] = Some(Nil)

  /**
   * This is the description of this Grouping in terms of a sequence of Every operations
   */
  protected var evs: List[Pipe => Every] = Nil
  protected var isReversed: Boolean = false

  protected var sortF: Option[Fields] = None
  def sorting = sortF
  /*
  * maxMF is the maximum index of a "middle field" allocated for mapReduceMap operations
  */
  private var maxMF: Int = 0

  private def getNextMiddlefield: String = {
    val out = "__middlefield__" + maxMF.toString
    maxMF += 1
    return out
  }

  private def tryAggregateBy(ab: AggregateBy, ev: Pipe => Every): Boolean = {
    // Concat if there if not none
    reds = reds.map(rl => ab :: rl)
    evs = ev :: evs
    return !reds.isEmpty
  }

  /**
   * Holds the number of reducers to use in the reduce stage of the groupBy/aggregateBy.
   * By default uses whatever value is set in the jobConf.
   */
  private var numReducers: Option[Int] = None

  /**
   * Holds an optional user-specified description to be used in .dot and MR step names.
   */
  private var descriptions: Seq[String] = Nil

  /**
   * Limit of number of keys held in SpillableTupleMap on an AggregateBy
   */
  private var spillThreshold: Option[Int] = None

  /**
   * Holds all the input fields that will be used in groupBy
   */
  private var projectFields: Option[Fields] = Some(groupFields)

  /**
   * Override the number of reducers used in the groupBy.
   */
  def reducers(r: Int) = {
    if (r > 0) {
      numReducers = Some(r)
    }
    this
  }

  /**
   * Override the description to be used in .dot and MR step names.
   */
  def setDescriptions(newDescriptions: Seq[String]) = {
    descriptions = newDescriptions
    this
  }

  /**
   * Override the spill threshold on AggregateBy
   */
  def spillThreshold(t: Int): GroupBuilder = {
    spillThreshold = Some(t)
    this
  }

  /**
   * This cancels map side aggregation
   * and forces everything to the reducers
   */
  def forceToReducers = {
    reds = None
    this
  }

  protected def overrideReducers(p: Pipe): Pipe = {
    numReducers.map { r => RichPipe.setReducers(p, r) }.getOrElse(p)
  }

  protected def overrideDescription(p: Pipe): Pipe = {
    RichPipe.setPipeDescriptions(p, descriptions)
  }

  /**
   * == Warning ==
   * This may significantly reduce performance of your job.
   * It kills the ability to do map-side aggregation.
   */
  def buffer(args: Fields)(b: Buffer[_]): GroupBuilder = {
    every(pipe => new Every(pipe, args, b))
  }

  /**
   * Prefer aggregateBy operations!
   */
  def every(ev: Pipe => Every): GroupBuilder = {
    projectFields = None // Assume we are keeping all outputs
    reds = None
    evs = ev :: evs
    this
  }

  /**
   * Prefer reduce or mapReduceMap. foldLeft will force all work to be
   * done on the reducers.  If your function is not associative and
   * commutative, foldLeft may be required.
   *
   * == Best Practice ==
   * Make sure init is an immutable object.
   *
   * == Note ==
   * Init needs to be serializable with Kryo (because we copy it for each
   * grouping to avoid possible errors using a mutable init object).
   */
  def foldLeft[X, T](fieldDef: (Fields, Fields))(init: X)(fn: (X, T) => X)(implicit setter: TupleSetter[X], conv: TupleConverter[T]): GroupBuilder = {
    val (inFields, outFields) = fieldDef
    conv.assertArityMatches(inFields)
    setter.assertArityMatches(outFields)
    val ag = new FoldAggregator[T, X](fn, init, outFields, conv, setter)
    val beforePF = projectFields
    every(pipe => new Every(pipe, inFields, ag))
    // Update projectFields, which makes sense in a fold, but invalidated on every
    projectFields = beforePF.map { Fields.merge(_, inFields) }
    this
  }

  /**
   * Type `T` is the type of the input field `(input to map, T => X)`
   *
   * Type `X` is the intermediate type, which your reduce function operates on
   * `(reduce is (X,X) => X)`
   *
   * Type `U` is the final result type, `(final map is: X => U)`
   *
   * The previous output goes into the reduce function on the left, like foldLeft,
   * so if your operation is faster for the accumulator to be on one side, be aware.
   */
  def mapReduceMap[T, X, U](fieldDef: (Fields, Fields))(mapfn: T => X)(redfn: (X, X) => X)(mapfn2: X => U)(implicit startConv: TupleConverter[T],
    middleSetter: TupleSetter[X],
    middleConv: TupleConverter[X],
    endSetter: TupleSetter[U]): GroupBuilder = {
    val (maybeSortedFromFields, maybeSortedToFields) = fieldDef
    //Check for arity safety:
    // To fields CANNOT have a sorting, or cascading gets unhappy:
    // TODO this may be fixed in cascading later
    val toFields = new Fields(asList(maybeSortedToFields): _*)
    val fromFields = new Fields(asList(maybeSortedFromFields): _*)
    startConv.assertArityMatches(fromFields)
    endSetter.assertArityMatches(toFields)
    // Update projectFields
    projectFields = projectFields.map { Fields.merge(_, fromFields) }
    val ag = new MRMAggregator[T, X, U](mapfn, redfn, mapfn2, toFields, startConv, endSetter)
    val ev = (pipe => new Every(pipe, fromFields, ag)): Pipe => Every
    assert(middleSetter.arity > 0,
      "The middle arity must have definite size, try wrapping in scala.Tuple1 if you need a hack")
    // Create the required number of middlefields based on the arity of middleSetter
    val middleFields = strFields(ScalaRange(0, middleSetter.arity).map { i => getNextMiddlefield })
    val mrmBy = new MRMBy[T, X, U](fromFields, middleFields, toFields,
      mapfn, redfn, mapfn2, startConv, middleSetter, middleConv, endSetter)
    tryAggregateBy(mrmBy, ev)
    this
  }

  /**
   * Corresponds to a Cascading Buffer
   * which allows you to stream through the data, keeping some, dropping, scanning, etc...
   * The iterator you are passed is lazy, and mapping will not trigger the
   * entire evaluation.  If you convert to a list (i.e. to reverse), you need to be aware
   * that memory constraints may become an issue.
   *
   * == Warning ==
   * Any fields not referenced by the input fields will be aligned to the first output,
   * and the final hadoop stream will have a length of the maximum of the output of this, and
   * the input stream.  So, if you change the length of your inputs, the other fields won't
   * be aligned.  YOU NEED TO INCLUDE ALL THE FIELDS YOU WANT TO KEEP ALIGNED IN THIS MAPPING!
   * POB: This appears to be a Cascading design decision.
   *
   * == Warning ==
   * mapfn needs to be stateless.  Multiple calls needs to be safe (no mutable
   * state captured)
   */
  def mapStream[T, X](fieldDef: (Fields, Fields))(mapfn: (Iterator[T]) => TraversableOnce[X])(implicit conv: TupleConverter[T], setter: TupleSetter[X]) = {
    val (inFields, outFields) = fieldDef
    //Check arity
    conv.assertArityMatches(inFields)
    setter.assertArityMatches(outFields)
    val b = new BufferOp[Unit, T, X]((),
      (u: Unit, it: Iterator[T]) => mapfn(it), outFields, conv, setter)
    every(pipe => new Every(pipe, inFields, b, defaultMode(inFields, outFields)))
  }

  def reverse: GroupBuilder = {
    assert(reds.isEmpty, "Cannot sort when reducing")
    assert(!isReversed, "Reverse called a second time! Only one allowed")
    isReversed = true
    this
  }

  /**
   * Analog of standard scanLeft (@see scala.collection.Iterable.scanLeft )
   * This invalidates map-side aggregation, forces all data to be transferred
   * to reducers.  Use only if you REALLY have to.
   *
   * == Best Practice ==
   * Make sure init is an immutable object.
   *
   * == Note ==
   * init needs to be serializable with Kryo (because we copy it for each
   * grouping to avoid possible errors using a mutable init object).
   *  We override the default implementation here to use Kryo to serialize
   *  the initial value, for immutable serializable inits, this is not needed
   */
  override def scanLeft[X, T](fieldDef: (Fields, Fields))(init: X)(fn: (X, T) => X)(implicit setter: TupleSetter[X], conv: TupleConverter[T]): GroupBuilder = {
    val (inFields, outFields) = fieldDef
    //Check arity
    conv.assertArityMatches(inFields)
    setter.assertArityMatches(outFields)
    val b = new BufferOp[X, T, X](init,
      // On scala 2.8, there is no scanLeft
      // On scala 2.9, their implementation creates an off-by-one bug with the unused fields
      (i: X, it: Iterator[T]) => new ScanLeftIterator(it, i, fn),
      outFields, conv, setter)
    every(pipe => new Every(pipe, inFields, b, defaultMode(inFields, outFields)))
  }

  def groupMode: GroupMode =
    (reds, evs, sortF) match {
      case (None, Nil, Some(_)) => IdentityMode // no reducers or everys, just a sort
      case (Some(Nil), Nil, _) => IdentityMode // no sort, just identity. used to shuffle data
      case (None, _, _) => GroupByMode
      case (Some(redList), _, None) => AggregateByMode // use map-side aggregation
      case _ => sys.error("Invalid GroupBuilder state: %s, %s, %s".format(reds, evs, sortF))
    }

  protected def groupedPipeOf(name: String, in: Pipe): GroupBy = {
    val gb: GroupBy = sortF match {
      case None => new GroupBy(name, in, groupFields)
      case Some(sf) => new GroupBy(name, in, groupFields, sf, isReversed)
    }
    overrideReducers(gb)
    overrideDescription(gb)
    gb
  }

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def schedule(name: String, pipe: Pipe): Pipe = {
    val maybeProjectedPipe = projectFields.map { pipe.project(_) }.getOrElse(pipe)
    groupMode match {
      case GroupByMode =>
        //In this case we cannot aggregate, so group:
        val start: Pipe = groupedPipeOf(name, maybeProjectedPipe)
        // Time to schedule the Every operations
        evs.foldRight(start) { (op: (Pipe => Every), p) => op(p) }

      case IdentityMode =>
        //This is the case where the group function is identity: { g => g }
        groupedPipeOf(name, pipe)

      case AggregateByMode =>
        //There is some non-empty AggregateBy to do:
        val redlist = reds.get
        val ag = new AggregateBy(name,
          maybeProjectedPipe,
          groupFields,
          spillThreshold.getOrElse(0), // cascading considers 0 to be the default
          redlist.reverse.toArray: _*)

        overrideReducers(ag.getGroupBy())
        overrideDescription(ag.getGroupBy())
        ag
    }
  }

  /**
   * This invalidates aggregateBy!
   */
  def sortBy(f: Fields): GroupBuilder = {
    reds = None
    val sort = sortF match {
      case None => f
      case Some(sf) => {
        sf.append(f)
        sf
      }
    }
    sortF = Some(sort)
    // Update projectFields
    projectFields = projectFields.map { Fields.merge(_, sort) }
    this
  }

  /**
   * This is convenience method to allow plugging in blocks
   * of group operations similar to `RichPipe.thenDo`
   */
  def thenDo(fn: (GroupBuilder) => GroupBuilder) = fn(this)

  /**
   * An identity function that keeps all the tuples. A hack to implement
   * groupAll and groupRandomly.
   */
  def pass: GroupBuilder = takeWhile(0) { (t: TupleEntry) => true }

  /**
   * beginning of block with access to expensive nonserializable state. The state object should
   * contain a function release() for resource management purpose.
   */
  def using[C <: { def release(): Unit }](bf: => C) = new {

    /**
     * mapStream with state.
     */
    def mapStream[T, X](fieldDef: (Fields, Fields))(mapfn: (C, Iterator[T]) => TraversableOnce[X])(implicit conv: TupleConverter[T], setter: TupleSetter[X]) = {
      val (inFields, outFields) = fieldDef
      //Check arity
      conv.assertArityMatches(inFields)
      setter.assertArityMatches(outFields)

      val b = new SideEffectBufferOp[Unit, T, C, X](
        (), bf,
        (u: Unit, c: C, it: Iterator[T]) => mapfn(c, it),
        new Function1[C, Unit] with java.io.Serializable { def apply(c: C): Unit = { c.release() } },
        outFields, conv, setter)
      every(pipe => new Every(pipe, inFields, b, defaultMode(inFields, outFields)))
    }
  }

}

/**
 * Scala 2.8 Iterators don't support scanLeft so we have to reimplement
 * The Scala 2.9 implementation creates an off-by-one bug with the unused fields in the Fields API
 */
class ScanLeftIterator[T, U](it: Iterator[T], init: U, fn: (U, T) => U) extends Iterator[U] with java.io.Serializable {
  protected var prev: Option[U] = None
  def hasNext: Boolean = { prev.isEmpty || it.hasNext }
  // Don't use pattern matching in a performance-critical section
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def next = {
    prev = prev.map { fn(_, it.next) }
      .orElse(Some(init))
    prev.get
  }
}

sealed private[scalding] abstract class GroupMode
private[scalding] case object AggregateByMode extends GroupMode
private[scalding] case object GroupByMode extends GroupMode
private[scalding] case object IdentityMode extends GroupMode
