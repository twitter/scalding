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

import com.twitter.scalding.serialization.JavaStreamEnrichments._

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

abstract class ComplexHelper[T] extends HasUnsafeCompareBinary[T] {
  def staticSize: Option[Int] = None

  protected def dynamicSizeWithoutLen(e: T): Option[Int]
  final def dynamicSize(e: T) =
    if (staticSize.isDefined) staticSize
    else
      dynamicSizeWithoutLen(e).map { e =>
        e + posVarIntSize(e)
      }
  final def unsafeSize(t: T): Option[Int] = dynamicSizeWithoutLen(t)

  /**
   * This is the worst case: we have to serialize in a side buffer
   * and then see how large it actually is. This happens for cases, like
   * string, where the cost to see the serialized size is not cheaper than
   * directly serializing.
   */
  private[this] def noLengthWrite(element: T, outerOutputStream: OutputStream): Unit = {
    // Start with pretty big buffers because reallocation will be expensive
    val baos = new ByteArrayOutputStream(512)
    unsafeWrite(baos, element)
    val len = baos.size
    outerOutputStream.writePosVarInt(len)
    baos.writeTo(outerOutputStream)
  }

  final override def write(into: OutputStream, e: T): Try[Unit] =
    try {
      if (staticSize.isDefined) {
        unsafeWrite(into, e)
      } else {
        val dynSiz = dynamicSizeWithoutLen(e)
        dynSiz match {
          case Some(innerSiz) =>

            into.writePosVarInt(innerSiz)
            unsafeWrite(into, e)
          case None =>
            noLengthWrite(e, into)
        }
      }
      Serialization.successUnit
    } catch {
      case NonFatal(e) =>
        Failure(e)
    }

  final def read(in: InputStream): Try[T] =
    try {
      if (staticSize.isEmpty)
        in.readPosVarInt

      Success(unsafeRead(in))
    } catch {
      case NonFatal(e) =>
        Failure(e)
    }

  final def compareBinary(inputStreamA: InputStream,
    inputStreamB: InputStream): OrderedSerialization.Result =
    try OrderedSerialization.resultFrom {
      val lenA = staticSize.getOrElse(inputStreamA.readPosVarInt)
      val lenB = staticSize.getOrElse(inputStreamB.readPosVarInt)

      val posStreamA = PositionInputStream(inputStreamA)
      val initialPositionA = posStreamA.position

      val posStreamB = PositionInputStream(inputStreamB)
      val initialPositionB = posStreamB.position

      val innerR = unsafeCompareBinary(posStreamA, posStreamB)

      posStreamA.seekToPosition(initialPositionA + lenA)
      posStreamB.seekToPosition(initialPositionB + lenB)
      innerR
    } catch {
      case NonFatal(e) =>
        OrderedSerialization.CompareFailure(e)
    }

}
