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
package com.twitter.scalding.db

import com.twitter.scalding.db.macros._
import com.twitter.scalding._
import java.util.Date
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.{ BeforeAndAfterAll, Suite }
import com.twitter.scalding.platform._
import org.scalacheck.Arbitrary

class SimpleJob(in: Iterable[MyVal], args: Args) extends Job(args) with java.io.Serializable {
  @transient implicit val dbs = AvailableDatabases()

  TypedPipe.from(in).write(VerticaSink[MyVal](null.asInstanceOf[ConnectionSpec],
    TableName("asdf"), SchemaName("schema"), None, None, true, Some(args("output_path"))))
}

class PlatformTest extends WordSpec with Matchers with BeforeAndAfterAll {

  private[this] val cluster = LocalCluster()

  override def beforeAll() {
    System.setProperty("cascading.update.skip", "true")

    cluster.synchronized {
      org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.FATAL)
      org.apache.log4j.Logger.getLogger("org.apache.hadoop.metrics2.util").setLevel(org.apache.log4j.Level.ERROR)
      org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.FATAL)

      cluster.initialize()
    }
    super.beforeAll()
  }

  "Vertica Binary should" should {

    "Match what vertica accepted" in {
      val jobFolder = "jobOutput"
      val expectedRemotePath = s"/tmp/$jobFolder"
      val args = Mode.putMode(cluster.mode, new Args(Map("output_path" -> List(expectedRemotePath))))
      val j = new SimpleJob(MyVal.sample, args)
      j.run

      val hmode = cluster.mode.asInstanceOf[HadoopMode]
      val conf = hmode.jobConf
      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
      val localPath = java.nio.file.Files.createTempDirectory("hadoopVerticaTest").toString
      // Copy down the local tmp outputs

      fs.copyToLocalFile(new org.apache.hadoop.fs.Path(expectedRemotePath), new org.apache.hadoop.fs.Path(localPath))

      val data = TestUtils.fetchLocalData(s"$localPath/$jobFolder", "part-00000")

      // This is a golden data set than de-serializing since our ground truth is what vertica will accept, and at this point
      // vertica is accepting what the macro is generating
      val expectedData: List[Byte] =
        List(78, 65, 84, 73, 86, 69, 10, -1, 13, 10, 0, 33, 0, 0, 0, 1, 0, 0, 7, 0, 8, 0, 0, 0, 8, 0, 0, 0, -1, -1, -1, -1, 1,
          0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
          48, 1, 0, 0, 0, 0, 0, 32, -56, -60, -2, -94, -4, -1, 0, 32, -56, -60, -2, -94, -4, -1, 38, 0, 0, 0, 8, 1, 0, 0, 0, 0, 0,
          0, 0, 74, -75, 79, -57, 99, 118, 36, 64, 1, 0, 0, 0, 49, 0, 0, -128, -97, -30, 18, -93, -4, -1, 0, -128, -97, -30, 18, -93,
          -4, -1, 38, 0, 0, 0, 8, 2, 0, 0, 0, 0, 0, 0, 0, 74, -75, 79, -57, 99, 118, 52, 64, 1, 0, 0, 0, 50, 1, 0, -32, 118, 0, 39, -93,
          -4, -1, 0, -32, 118, 0, 39, -93, -4, -1).map(_.toByte)
      assert(data === expectedData)
    }
  }

  override def afterAll() {
    try super.afterAll()
    finally {
      cluster.synchronized {
        cluster.shutdown()
      }
    }
  }

}
