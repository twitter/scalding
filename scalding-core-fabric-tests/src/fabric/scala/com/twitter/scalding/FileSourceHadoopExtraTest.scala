package com.twitter.scalding

import org.apache.hadoop.conf.Configuration
import org.scalatest.{ Matchers, WordSpec }

class FileSourceHadoopExtraTest /* logically extends FileSourceTest */ extends WordSpec with Matchers {
  import Dsl._

  def makeClusterMode(strictSources: Boolean) =
    Mode(Args(Seq("--autoCluster") ++ (if (strictSources) Seq[String]() else Seq("--tool.partialok"))), new Configuration)

  "invalid source input" should {
    "Throw in validateTaps in strict mode" in {
      val e = intercept[InvalidSourceException] {
        TestInvalidFileSource.validateTaps(makeClusterMode(true))
      }
      assert(e.getMessage.endsWith("Data is missing from one or more paths in: List(invalid_hdfs_path)"))
    }

    "Throw in validateTaps in non-strict mode" in {
      val e = intercept[InvalidSourceException] {
        TestInvalidFileSource.validateTaps(makeClusterMode(false))
      }
      assert(e.getMessage.endsWith("No good paths in: List(invalid_hdfs_path)"))
    }

    "Throw in toIterator because no data is present in strict mode" in {
      val e = intercept[InvalidSourceException] {
        TestInvalidFileSource.toIterator(Config.default, makeClusterMode(true))
      }
      assert(e.getMessage.endsWith("Data is missing from one or more paths in: List(invalid_hdfs_path)"))
    }

    "Throw in toIterator because no data is present in non-strict mode" in {
      val e = intercept[InvalidSourceException] {
        TestInvalidFileSource.toIterator(Config.default, makeClusterMode(false))
      }
      assert(e.getMessage.endsWith("No good paths in: List(invalid_hdfs_path)"))
    }
  }

}
