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
package com.twitter.scalding.serialization
import com.twitter.chill.{ Externalizer => ChillExtern }

import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.serializers.JavaSerializer

import com.twitter.chill.config.ScalaAnyRefMapConfig
/**
 * We need to control the Kryo created
 */
object Externalizer {
  def apply[T](t: T): Externalizer[T] = {
    val e = new Externalizer[T]
    e.set(t)
    e
  }
}

@DefaultSerializer(classOf[JavaSerializer])
class Externalizer[T] extends ChillExtern[T] {
  protected override def kryo =
    new KryoHadoop(ScalaAnyRefMapConfig(Map("scalding.kryo.setreferences" -> "true")))
}

