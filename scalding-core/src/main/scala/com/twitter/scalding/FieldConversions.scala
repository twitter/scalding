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

import cascading.tuple.Fields
import cascading.pipe.Pipe
import com.esotericsoftware.kryo.DefaultSerializer

import java.util.Comparator
import scala.annotation.tailrec
import scala.collection.JavaConverters._

trait LowPriorityFieldConversions {

  protected def anyToFieldArg(f: Any): Comparable[_] = f match {
    case x: Symbol => x.name
    case y: String => y
    case z: java.lang.Integer => z
    case v: Enumeration#Value => v.toString
    case fld: Field[_] => fld.id
    case flds: Fields => {
      if (flds.size == 1) {
        flds.get(0)
      } else {
        throw new Exception("Cannot convert Fields(" + flds.toString + ") to a single fields arg")
      }
    }
    case w => throw new Exception("Could not convert: " + w.toString + " to Fields argument")
  }

  /**
   * Handles treating any TupleN as a Fields object.
   * This is low priority because List is also a Product, but this method
   * will not work for List (because List is Product2(head, tail) and so
   * productIterator won't work as expected.
   * Lists are handled by an implicit in FieldConversions, which have
   * higher priority.
   */
  implicit def productToFields(f: Product): Fields = {
    val fields = new Fields(f.productIterator.map { anyToFieldArg }.toSeq: _*)
    f.productIterator.foreach {
      case field: Field[_] => fields.setComparator(field.id, field.ord)
      case _ =>
    }
    fields
  }
}

trait FieldConversions extends LowPriorityFieldConversions {

  // Cascading Fields are either java.lang.String or java.lang.Integer, both are comparable.
  def asList(f: Fields): List[Comparable[_]] = {
    f.iterator.asScala.toList.asInstanceOf[List[Comparable[_]]]
  }
  // Cascading Fields are either java.lang.String or java.lang.Integer, both are comparable.
  def asSet(f: Fields): Set[Comparable[_]] = asList(f).toSet

  // TODO get the comparator also
  def getField(f: Fields, idx: Int): Fields = { new Fields(f.get(idx)) }

  def hasInts(f: Fields): Boolean = f.iterator.asScala.exists { _.isInstanceOf[java.lang.Integer] }

  /**
   * Rather than give the full power of cascading's selectors, we have
   * a simpler set of rules encoded below:
   * 1) if the input is non-definite (ALL, GROUP, ARGS, etc...) ALL is the output.
   *      Perhaps only fromFields=ALL will make sense
   * 2) If one of from or to is a strict super set of the other, SWAP is used.
   * 3) If they are equal, REPLACE is used.
   * 4) Otherwise, ALL is used.
   */
  def defaultMode(fromFields: Fields, toFields: Fields): Fields = {
    if (toFields.isArguments || (fromFields.isAll && toFields.isAll)) {
      // 1. In this case we replace the input with the output or:
      // 2. if you go from all to all, you must mean replace (ALL would fail at the cascading layer)
      Fields.REPLACE
    } else if (fromFields.size == 0) {
      //This is all the UNKNOWN, ALL, etc...
      Fields.ALL
    } else {
      val fromSet = asSet(fromFields)
      val toSet = asSet(toFields)
      (fromSet.subsetOf(toSet), toSet.subsetOf(fromSet)) match {
        case (true, true) => Fields.REPLACE //equal
        case (true, false) => Fields.SWAP //output super set, replaces input
        case (false, true) => Fields.SWAP //throw away some input
        /*
        * the next case is that they are disjoint or have some nontrivial intersection
        * if disjoint, everything is fine.
        * if they intersect, it is ill-defined and cascading is going to throw an error BEFORE
        *   starting the flow.
        */
        case (false, false) => Fields.ALL
      }
    }
  }

  //Single entry fields:
  implicit def unitToFields(u: Unit): Fields = Fields.NONE // linter:ignore
  implicit def intToFields(x: Int): Fields = new Fields(new java.lang.Integer(x))
  implicit def integerToFields(x: java.lang.Integer): Fields = new Fields(x)
  implicit def stringToFields(x: String): Fields = new Fields(x)
  implicit def enumValueToFields(x: Enumeration#Value): Fields = new Fields(x.toString)
  /**
   * '* means Fields.ALL, otherwise we take the .name
   */
  implicit def symbolToFields(x: Symbol): Fields = {
    if (x == '*) {
      Fields.ALL
    } else {
      new Fields(x.name)
    }
  }
  implicit def fieldToFields(f: Field[_]): RichFields = RichFields(f)

  @tailrec
  final def newSymbol(avoid: Set[Symbol], guess: Symbol, trial: Int = 0): Symbol = {
    if (!avoid(guess)) {
      //We are good:
      guess
    } else if (trial == 0) {
      newSymbol(avoid, guess, 1)
    } else {
      val newGuess = Symbol(guess.name + trial.toString)
      if (!avoid(newGuess)) {
        newGuess
      } else {
        newSymbol(avoid, guess, trial + 1)
      }
    }
  }

  final def ensureUniqueFields(left: Fields, right: Fields, rightPipe: Pipe): (Fields, Pipe) = {
    val leftSet = asSet(left)
    val collisions = asSet(left) & asSet(right)
    if (collisions.isEmpty) {
      (right, rightPipe)
    } else {
      // Rename the collisions with random integer names:
      val leftSetSyms = leftSet.map { f => Symbol(f.toString) }
      val (_, reversedRename) = asList(right).map { f => Symbol(f.toString) }
        .foldLeft((leftSetSyms, List[Symbol]())) { (takenRename, name) =>
          val (taken, renames) = takenRename
          val newName = newSymbol(taken, name)
          (taken + newName, newName :: renames)
        }
      val newRight = fields(reversedRename.reverse) // We pushed in as a stack, so we need to reverse
      (newRight, RichPipe(rightPipe).rename(right -> newRight))
    }
  }

  /**
   * Multi-entry fields.  This are higher priority than Product conversions so
   * that List will not conflict with Product.
   */
  implicit def fromEnum[T <: Enumeration](enumeration: T): Fields =
    new Fields(enumeration.values.toList.map { _.toString }: _*)

  implicit def fields[T <: TraversableOnce[Symbol]](f: T): Fields = new Fields(f.toSeq.map(_.name): _*)
  implicit def strFields[T <: TraversableOnce[String]](f: T): Fields = new Fields(f.toSeq: _*)
  implicit def intFields[T <: TraversableOnce[Int]](f: T): Fields = {
    new Fields(f.toSeq.map { new java.lang.Integer(_) }: _*)
  }
  implicit def fieldFields[T <: TraversableOnce[Field[_]]](f: T): RichFields = RichFields(f.toSeq)
  /**
   * Useful to convert f : Any* to Fields.  This handles mixed cases ("hey", 'you).
   * Not sure we should be this flexible, but given that Cascading will throw an
   * exception before scheduling the job, I guess this is okay.
   */
  implicit def parseAnySeqToFields[T <: TraversableOnce[Any]](anyf: T): Fields = {
    val fields = new Fields(anyf.toSeq.map { anyToFieldArg }: _*)
    anyf.foreach {
      case field: Field[_] => fields.setComparator(field.id, field.ord)
      case _ =>
    }
    fields
  }

  //Handle a pair generally:
  implicit def tuple2ToFieldsPair[T, U](pair: (T, U))(implicit tf: T => Fields, uf: U => Fields): (Fields, Fields) = {
    val f1 = tf(pair._1)
    val f2 = uf(pair._2)
    (f1, f2)
  }
  /**
   * We can't set the field Manifests because cascading doesn't (yet) expose field type information
   * in the Fields API.
   */
  implicit def fieldsToRichFields(fields: Fields): RichFields = {
    if (!fields.isDefined) {
      // TODO We could provide a reasonable conversion here by designing a rich type hierarchy such as
      // Fields
      //   RichFieldSelector
      //     RichFields
      //     AllFields
      //     ArgsFields
      // etc., together with an implicit for converting a Fields instance to a RichFieldSelector
      // of the appropriate type
      sys.error("virtual Fields cannot be converted to RichFields")
    }

    // This bit is kludgy because cascading provides different interfaces for extracting
    // IDs and Comparators from a Fields instance.  (The IDs are only available
    // "one at a time" by querying for a specific index, while the Comparators are only
    // available "all at once" by calling getComparators.)

    new RichFields(asList(fields).zip(fields.getComparators).map {
      case (id: Comparable[_], comparator: Comparator[_]) => id match {
        case x: java.lang.Integer => IntField(x)(Ordering.comparatorToOrdering(comparator), None)
        case y: String => StringField(y)(Ordering.comparatorToOrdering(comparator), None)
        case z => sys.error("not expecting object of type " + z.getClass + " as field name")
      }
    })
  }

}

// An extension of the cascading Fields class that provides easy conversion to a List[Field[_]].
// With FieldConversions._ in scope, the following will work:
//
// val myFields: Fields = ...
// myFields.toFieldList

case class RichFields(val toFieldList: List[Field[_]]) extends Fields(toFieldList.map { _.id }: _*) {
  toFieldList.foreach { field: Field[_] => setComparator(field.id, field.ord) }
}

object RichFields {
  def apply(f: Field[_]*) = new RichFields(f.toList)
  def apply(f: Traversable[Field[_]]) = new RichFields(f.toList)

}

sealed trait Field[T] extends java.io.Serializable {
  def id: Comparable[_]
  def ord: Ordering[T]
  def mf: Option[Manifest[T]]
}

@DefaultSerializer(classOf[serialization.IntFieldSerializer])
case class IntField[T](override val id: java.lang.Integer)(implicit override val ord: Ordering[T], override val mf: Option[Manifest[T]]) extends Field[T]

@DefaultSerializer(classOf[serialization.StringFieldSerializer])
case class StringField[T](override val id: String)(implicit override val ord: Ordering[T], override val mf: Option[Manifest[T]]) extends Field[T]

object Field {
  def apply[T](index: Int)(implicit ord: Ordering[T], mf: Manifest[T]) = IntField[T](index)(ord, Some(mf))
  def apply[T](name: String)(implicit ord: Ordering[T], mf: Manifest[T]) = StringField[T](name)(ord, Some(mf))
  def apply[T](symbol: Symbol)(implicit ord: Ordering[T], mf: Manifest[T]) = StringField[T](symbol.name)(ord, Some(mf))

  def singleOrdered[T](name: String)(implicit ord: Ordering[T]): Fields = {
    val f = new Fields(name)
    f.setComparator(name, ord)
    f
  }
}
