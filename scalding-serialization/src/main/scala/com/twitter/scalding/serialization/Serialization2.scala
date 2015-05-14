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

import java.io.{ InputStream, OutputStream }

import scala.util.{ Failure, Success, Try }

class Serialization2[A, B](val serA: Serialization[A], val serB: Serialization[B]) extends Serialization[(A, B)] {
  override def hash(x: (A, B)) = {
    import MurmurHashUtils._
    val h1 = mixH1(seed, serA.hash(x._1))
    val h2 = mixH1(h1, serB.hash(x._2))
    fmix(h2, 2)
  }
  override def equiv(x: (A, B), y: (A, B)): Boolean =
    serA.equiv(x._1, y._1) && serB.equiv(x._2, y._2)

  override def read(in: InputStream): Try[(A, B)] = {
    val a = serA.read(in)
    val b = serB.read(in)
    (a, b) match {
      case (Success(a), Success(b)) => Success((a, b))
      case (Failure(e), _) => Failure(e)
      case (_, Failure(e)) => Failure(e)
    }
  }

  override def write(out: OutputStream, a: (A, B)): Try[Unit] = {
    val resA = serA.write(out, a._1)
    if (resA.isSuccess) serB.write(out, a._2)
    else resA
  }

  override val staticSize = for {
    a <- serA.staticSize
    b <- serB.staticSize
  } yield a + b

  override def dynamicSize(t: (A, B)) = if (staticSize.isDefined) staticSize
  else for {
    a <- serA.dynamicSize(t._1)
    b <- serB.dynamicSize(t._2)
  } yield a + b
}

class OrderedSerialization2[A, B](val ordA: OrderedSerialization[A],
  val ordB: OrderedSerialization[B]) extends Serialization2[A, B](ordA, ordB) with OrderedSerialization[(A, B)] {
  override def compare(x: (A, B), y: (A, B)) = {
    val ca = ordA.compare(x._1, y._1)
    if (ca != 0) ca
    else ordB.compare(x._2, y._2)
  }
  override def compareBinary(a: InputStream, b: InputStream) = {
    // This mutates the buffers and advances them. Only keep reading if they are different
    val cA = ordA.compareBinary(a, b)
    // we have to read the second ones to skip
    val cB = ordB.compareBinary(a, b)
    cA match {
      case OrderedSerialization.Equal => cB
      case f @ OrderedSerialization.CompareFailure(_) => f
      case _ => cA // the first is not equal
    }
  }
}
