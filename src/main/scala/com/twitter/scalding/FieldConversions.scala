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

import scala.collection.JavaConversions._
import scala.collection.mutable.WrappedArray

trait LowPriorityFieldConversions {

  protected def anyToFieldArg(f : Any) : Comparable[_] = {
    f match {
        case x : Symbol => x.name
        case y : String => y
        case z : java.lang.Integer => z
        case w => throw new Exception("Could not convert: " + w.toString + " to Fields argument")
    }
  }

  /**
  * Handles treating any TupleN as a Fields object.
  * This is low priority because List is also a Product, but this method
  * will not work for List (because List is Product2(head, tail) and so
  * productIterator won't work as expected.
  * Lists are handled by an implicit in FieldConversions, which have
  * higher priority.
  */
  implicit def productToFields( f : Product ) = {
    new Fields(f.productIterator.map { anyToFieldArg }.toSeq :_* )
  }
}

trait FieldConversions extends LowPriorityFieldConversions {

  // Cascading Fields are either java.lang.String or java.lang.Integer, both are comparable.
  def asList(f : Fields) : List[Comparable[_]] = {
    f.iterator.toList.asInstanceOf[List[Comparable[_]]]
  }
  // Cascading Fields are either java.lang.String or java.lang.Integer, both are comparable.
  def asSet(f : Fields) : Set[Comparable[_]] = asList(f).toSet

  def hasInts(f : Fields) = {
    f.iterator.find { _.isInstanceOf[java.lang.Integer] }.isDefined
  }

  /**
  * Rather than give the full power of cascading's selectors, we have
  * a simpler set of rules encoded below:
  * 1) if the input is non-definite (ALL, GROUP, ARGS, etc...) ALL is the output.
  *      Perhaps only fromFields=ALL will make sense
  * 2) If one of from or to is a strict super set of the other, SWAP is used.
  * 3) If they are equal, REPLACE is used.
  * 4) Otherwise, ALL is used.
  */
  def defaultMode(fromFields : Fields, toFields : Fields) : Fields = {
    if(toFields.isArguments) {
      //In this case we replace the input with the output
      Fields.REPLACE
    }
    else if( fromFields.size == 0 ) {
      //This is all the UNKNOWN, ALL, etc...
      Fields.ALL
    }
    else {
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
  implicit def unitToFields(u : Unit) = Fields.NONE
  implicit def intToFields(x : Int) = new Fields(new java.lang.Integer(x))
  implicit def integerToFields(x : java.lang.Integer) = new Fields(x)
  implicit def stringToFields(x : String) = new Fields(x)
  /**
  * '* means Fields.ALL, otherwise we take the .name
  */
  implicit def symbolToFields(x : Symbol) = {
    if(x == '*)  {
      Fields.ALL
    }
    else {
      new Fields(x.name)
    }
  }

  /**
   * Multi-entry fields.  This are higher priority than Product conversions so
   * that List will not conflict with Product.
   */
  implicit def fields[T <: TraversableOnce[Symbol]](f : T) = new Fields(f.toSeq.map(_.name) : _*)
  implicit def strFields[T <: TraversableOnce[String]](f : T) = new Fields(f.toSeq : _*)
  implicit def intFields[T <: TraversableOnce[Int]](f : T) = {
    new Fields(f.toSeq.map { new java.lang.Integer(_) } : _*)
  }
  /**
  * Useful to convert f : Any* to Fields.  This handles mixed cases ("hey", 'you).
  * Not sure we should be this flexible, but given that Cascading will throw an
  * exception before scheduling the job, I guess this is okay.
  */
  implicit def parseAnySeqToFields[T <: TraversableOnce[Any]](anyf : T) = {
    new Fields(anyf.toSeq.map { anyToFieldArg } : _* )
  }

  //Handle a pair generally:
  implicit def tuple2ToFieldsPair[T,U]( pair : (T,U) )
    (implicit tf : T => Fields, uf : U => Fields) : (Fields,Fields) = {
    val f1 = tf(pair._1)
    val f2 = uf(pair._2)
    (f1, f2)
  }
}
