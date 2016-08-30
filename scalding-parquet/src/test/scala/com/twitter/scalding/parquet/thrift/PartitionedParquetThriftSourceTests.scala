package com.twitter.scalding.parquet.thrift

import java.io.File

import com.twitter.scalding._
import com.twitter.scalding.parquet.thrift_java.test.Address
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.thrift.ThriftParquetReader

import org.scalatest.{ Matchers, WordSpec }

object PartitionedParquetThriftTestSources {
  val path = "/a/path"
  val partitionSource = PartitionedParquetThriftSource[String, Address](path, "%s")
}

class PartitionedParquetThriftWriteJob(args: Args) extends Job(args) {
  import PartitionedParquetThriftTestSources._
  val input = Seq(new Address("123 Embarcadero", "94111"), new Address("123 E 79th St", "10075"), new Address("456 W 80th St", "10075"))

  TypedPipe.from(input)
    .map { address => (address.getZip, address) }
    .write(partitionSource)
}

class PartitionedParquetThriftSourceTests extends WordSpec with Matchers {
  import PartitionedParquetThriftTestSources._

  def validate(path: Path, expectedAddresses: Address*) = {
    val parquetReader: ParquetReader[Address] =
      ThriftParquetReader.build(path).withThriftClass(classOf[Address]).build()
    Stream.continually(parquetReader.read).takeWhile(_ != null).toArray shouldBe expectedAddresses
  }

  "PartitionedParquetThriftSource" should {
    "write out partitioned thrift objects" in {
      var job: Job = null;
      def buildJob(args: Args): Job = {
        job = new PartitionedParquetThriftWriteJob(args)
        job
      }
      JobTest(buildJob(_))
        .runHadoop
        .finish()

      val testMode = job.mode.asInstanceOf[HadoopTest]

      val directory = new File(testMode.getWritePathFor(partitionSource))

      directory.listFiles().map({ _.getName() }).toSet shouldBe Set("94111", "10075")

      // check that the partitioning is done correctly by zipcode
      validate(new Path(directory.getPath + "/94111/part-00000-00000-m-00000.parquet"),
        new Address("123 Embarcadero", "94111"))
      validate(new Path(directory.getPath + "/10075/part-00000-00001-m-00000.parquet"),
        new Address("123 E 79th St", "10075"), new Address("456 W 80th St", "10075"))
    }
  }
}