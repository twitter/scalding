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
package com.twitter.scalding

import cascading.tuple.hadoop.SerializationToken

object CascadingTokenUpdater {
  private final val lowestAllowed = 128 // cascading rules

  // Take a cascading string of tokens and turns it into a map
  // from token index to class
  def parseTokens(tokClass: String): Map[Int, String] =
    if (tokClass == null || tokClass.isEmpty)
      Map[Int, String]()
    else
      tokClass
        .split(",")
        .toIterator
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(_.split("="))
        .filter(_.length == 2)
        .map { ary => (ary(0).toInt, ary(1)) }
        .toMap

  // does the inverse of the previous function, given a Map of index to class
  // return the cascading token format for it
  private def toksToString(m: Map[Int, String]): String =
    m.map { case (tok, clazz) => s"$tok=$clazz" }.mkString(",")

  // Given the map of already assigned tokens, what is the next available one
  private def firstAvailableToken(m: Map[Int, String]): Int =
    if (m.isEmpty) lowestAllowed
    else scala.math.max(m.keys.max + 1, lowestAllowed)

  // Given the first free token spot
  // assign each of the class names given to al the subsequent
  // positions
  private def assignTokens(first: Int, names: Iterable[String]): Map[Int, String] =
    names.foldLeft((first, Map[Int, String]())) { (idMap, clz) =>
      val (id, m) = idMap
      (id + 1, m + (id -> clz))
    }._2

  def update(config: Config, clazzes: Set[Class[_]]): Config = {
    val toks = config.getCascadingSerializationTokens

    val serializations = config.get(Config.IoSerializationsKey).getOrElse("")
    val fromSerializations: Seq[String] = if (serializations.isEmpty)
      Seq.empty
    else
      for {
        serialization <- serializations.split(",")
        clazz = Class.forName(serialization)
        tokenAnnotation = clazz.getAnnotation(classOf[SerializationToken])
        if tokenAnnotation != null
        className <- tokenAnnotation.classNames()
      } yield {
        className
      }

    // We don't want to assign tokens to classes already in the map
    val newClasses: Iterable[String] = clazzes.map { _.getName } -- fromSerializations -- toks.values

    config + (Config.CascadingSerializationTokens -> toksToString(toks ++ assignTokens(firstAvailableToken(toks), newClasses)))
  }

}
