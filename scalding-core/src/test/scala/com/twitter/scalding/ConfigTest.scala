/*
Copyright 2014 Twitter, Inc.

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

import org.scalatest.{ WordSpec, Matchers }
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen._

import scala.util.Success

class ConfigTest extends WordSpec with Matchers {
  "A Config" should {
    "cascadingAppJar works" in {
      val cls = getClass
      Config.default.setCascadingAppJar(cls)
        .getCascadingAppJar should contain (Success(cls))
    }
    "default has serialization set" in {
      val sers = Config.default.get("io.serializations").get.split(",").toList
      sers.last shouldBe (classOf[com.twitter.chill.hadoop.KryoSerialization].getName)
    }
    "default has chill configured" in {
      Config.default.get(com.twitter.chill.config.ConfiguredInstantiator.KEY) should not be empty
    }
    "setting timestamp twice does not change it" in {
      val date = RichDate.now
      val (oldDate, newConf) = Config.empty.maybeSetSubmittedTimestamp(date)
      oldDate shouldBe empty
      newConf.getSubmittedTimestamp should contain (date)
      val (stillOld, new2) = newConf.maybeSetSubmittedTimestamp(date + Seconds(1))
      stillOld should contain (date)
      new2 shouldBe newConf
    }
  }
}

object ConfigProps extends Properties("Config") {
  implicit def arbConfig: Arbitrary[Config] =
    Arbitrary(Arbitrary.arbitrary[Map[String, String]].map(Config(_)))

  property(".+(k, v).get(k) == Some(v)") = forAll { (c: Config, k: String, v: String) =>
    (c + (k, v)).get(k) == Some(v)
  }
  property(".-(k).get(k) == None") = forAll { (c: Config, k: String) =>
    (c - k).get(k) == None
  }
  property("++ unions keys") = forAll { (c1: Config, c2: Config) =>
    (c1 ++ c2).toMap.keySet == (c1.toMap.keySet | c2.toMap.keySet)
  }
  property("++ == c2.orElse(c1)") = forAll { (c1: Config, c2: Config, keys: Set[String]) =>
    val merged = c1 ++ c2
    val testKeys = c1.toMap.keySet | c2.toMap.keySet ++ keys
    testKeys.forall { k => merged.get(k) == c2.get(k).orElse(c1.get(k)) }
  }
}
