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

import cascading.tuple.{TupleEntry,Tuple=>CTuple}

import org.specs._

class TupleTest extends Specification {
  noDetailedDiffs() //Fixes issue for scala 2.9

  def get[T](ctup : CTuple)(implicit tc : TupleConverter[T]) = tc(new TupleEntry(ctup))
  def set[T](t : T)(implicit ts : TupleSetter[T]) : CTuple = ts(t)

  def arityConvMatches[T](t : T, ar : Int)(implicit tc : TupleConverter[T]) : Boolean = {
    tc.arity == ar
  }
  def aritySetMatches[T](t : T, ar : Int)(implicit tc : TupleSetter[T]) : Boolean = {
    tc.arity == ar
  }

  def roundTrip[T](t : T)(implicit tc : TupleConverter[T], ts : TupleSetter[T]) : Boolean = {
    tc(new TupleEntry(ts(t))) == t
  }

  "TupleConverters" should {

    "TupleGetter should work as a type-class" in {
      val emptyTup = new CTuple
      val ctup = new CTuple("hey",new java.lang.Long(2), new java.lang.Integer(3), emptyTup)
      TupleGetter.get[String](ctup, 0) must be_==("hey")
      TupleGetter.get[Long](ctup, 1) must be_==(2L)
      TupleGetter.get[Int](ctup, 2) must be_==(3)
      TupleGetter.get[CTuple](ctup, 3) must be_==(emptyTup)
    }

    "get primitives out of cascading tuples" in {
      val ctup = new CTuple("hey",new java.lang.Long(2), new java.lang.Integer(3))
      get[(String,Long,Int)](ctup) must be_==(("hey",2L,3))

      roundTrip[Int](3) must beTrue
      arityConvMatches(3,1) must beTrue
      aritySetMatches(3,1) must beTrue
      roundTrip[Long](42L) must beTrue
      arityConvMatches(42L,1) must beTrue
      aritySetMatches(42L,1) must beTrue
      roundTrip[String]("hey") must beTrue
      arityConvMatches("hey",1) must beTrue
      aritySetMatches("hey",1) must beTrue
      roundTrip[(Int,Int)]((4,2)) must beTrue
      arityConvMatches((2,3),2) must beTrue
      aritySetMatches((2,3),2) must beTrue
    }
    "get non-primitives out of cascading tuples" in {
      val ctup = new CTuple(None,List(1,2,3), 1->2 )
      get[(Option[Int],List[Int],(Int,Int))](ctup) must be_==((None,List(1,2,3), 1->2 ))

      roundTrip[(Option[Int],List[Int])]((Some(1),List())) must beTrue
      arityConvMatches((None,Nil),2) must beTrue
      aritySetMatches((None,Nil),2) must beTrue

      arityConvMatches(None,1) must beTrue
      aritySetMatches(None,1) must beTrue
      arityConvMatches(List(1,2,3),1) must beTrue
      aritySetMatches(List(1,2,3),1) must beTrue
    }
    "deal with AnyRef" in {
      val ctup = new CTuple(None,List(1,2,3), 1->2 )
      get[(AnyRef,AnyRef,AnyRef)](ctup) must be_==((None,List(1,2,3), 1->2 ))
      get[AnyRef](new CTuple("you")) must be_==("you")

      roundTrip[AnyRef]("hey") must beTrue
      roundTrip[(AnyRef,AnyRef)]((Nil,Nil)) must beTrue
      arityConvMatches[(AnyRef,AnyRef)](("hey","you"),2) must beTrue
      aritySetMatches[(AnyRef,AnyRef)](("hey","you"),2) must beTrue
    }
  }
}
