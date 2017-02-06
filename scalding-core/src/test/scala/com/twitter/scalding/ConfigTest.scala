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

import com.twitter.scalding.filecache.{ HadoopCachedFile, URIHasher }
import java.net.URI
import org.apache.hadoop.mapreduce.MRJobConfig

import org.scalatest.{ WordSpec, Matchers }
import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

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
    "adding UniqueIDs works" in {
      assert(Config.empty.getUniqueIds.size === 0)
      val (id, conf) = Config.empty.ensureUniqueId
      assert(conf.getUniqueIds === (Set(id)))
    }
    "roundtrip Args" in {
      val config = Config.empty
      val args = Args(Array("--hello", "party people"))

      assert(config.setArgs(args).getArgs === args)
    }
    "throw when Args has been manually modified" in {
      val config = Config.empty + (Config.ScaldingJobArgsSerialized -> "  ")
      intercept[RuntimeException](config.getArgs)
    }
    "Default serialization should have tokens" in {
      Config.default.getCascadingSerializationTokens should not be empty
      Config.default.getCascadingSerializationTokens
        .values
        .map(Class.forName)
        .filter(c => c.isPrimitive || c.isArray) shouldBe empty

      Config.empty.getCascadingSerializationTokens shouldBe empty

      // tokenClasses are a subset that don't include primitives or arrays.
      val tokenClasses = Config.default.getCascadingSerializationTokens.values.toSet
      val kryoClasses = Config.default.getKryoRegisteredClasses.map(_.getName)
      // Tokens are a subset of Kryo registered classes
      (kryoClasses & tokenClasses) shouldBe tokenClasses
      // the only Kryo classes we don't assign tokens for are the primitives + array
      (kryoClasses -- tokenClasses).forall { c =>
        // primitives cannot be forName'd
        val prim = Set(classOf[Boolean], classOf[Byte], classOf[Short],
          classOf[Int], classOf[Long], classOf[Float], classOf[Double], classOf[Char], classOf[Unit])
          .map(_.getName)

        prim(c) || Class.forName(c).isArray
      } shouldBe true
    }
    "addDistributedCacheFile works" in {
      val (cachedFile, path) = ConfigTest.makeCachedFileAndPath("test.txt")

      Config
        .empty
        .addDistributedCacheFiles(cachedFile)
        .get(MRJobConfig.CACHE_FILES) shouldBe Some(path)
    }
    "multiple addDistributedCacheFile work" in {
      val (cachedFileFirst, pathFirst) = ConfigTest.makeCachedFileAndPath("first.txt")
      val (cachedFileSecond, pathSecond) = ConfigTest.makeCachedFileAndPath("second.txt")

      Config
        .empty
        .addDistributedCacheFiles(cachedFileFirst, cachedFileSecond)
        .get(MRJobConfig.CACHE_FILES) shouldBe Some(s"$pathFirst,$pathSecond")

      Config
        .empty
        .addDistributedCacheFiles(cachedFileFirst)
        .addDistributedCacheFiles(cachedFileSecond)
        .get(MRJobConfig.CACHE_FILES) shouldBe Some(s"$pathFirst,$pathSecond")
    }
  }
}

object ConfigTest {
  def makeCachedFileAndPath(name: String): (HadoopCachedFile, String) = {
    val uriString = s"hdfs://foo.example:1234/path/to/the/stuff/$name"
    val uri = new URI(uriString)
    val hashHex = URIHasher(uri)
    val hashedFilename = hashHex + s"-$name"
    val cachedFile = HadoopCachedFile(uri)

    (cachedFile, s"$uriString#$hashedFilename")
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
  property("adding many UniqueIDs works") = forAll { (l: List[String]) =>
    val uids = l.filterNot { s => s.isEmpty || s.contains(",") }.map(UniqueID(_))
    (uids.foldLeft(Config.empty) { (conf, id) =>
      conf.addUniqueId(id)
    }.getUniqueIds == uids.toSet)
  }
}
