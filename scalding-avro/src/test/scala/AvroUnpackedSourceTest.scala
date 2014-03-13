package com.twitter.scalding.avro

import com.twitter.scalding._

import org.specs.Specification
import org.specs.ScalaCheck
import org.scalacheck.Prop
import java.io.File
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.file.DataFileWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf



/** Example job that extracts an Avro source to tuples */
class SimpleUnpackedAvroJob(args: Args) extends Job(args) {
  UnpackedAvroSource(args("input"), MyAvroRecord.SCHEMA$)
    .read
    .map('price -> 'priceEUR) { (price: Double) => price * 1.5 }
    .project('priceEUR)
    .write(Tsv(args("output")))
}

/** Example job that maps an Avro source to the generated class */
class SimplePackedAvroJob(args: Args) extends Job(args) {
  PackedAvroSource[MyAvroRecord](args("input"))
    .read
    .map(0 -> 0) { record: MyAvroRecord =>
      record.setPrice(record.getPrice * 1.5)
      record.getPrice
    }
    .write(Tsv(args("output")))
}

/** Illustrates how to unit test Avro jobs */
class AvroUnpackedSourceTest extends Specification with ScalaCheck {

  "Running M/R jobs on Avro data" should {
    "work for unpacked sources" in {
      val prop = Prop.forAll(MyAvroRecordGenerators.myAvroGen) { (record: MyAvroRecord) =>
        var res: Double = 0.0

        val tempFolder = new File(System.getProperty("java.io.tmpdir"))
        val tempInput = new File(tempFolder, "input")
        tempInput.mkdirs
        val tempOutput = new File(tempFolder,"output")
        tempOutput.mkdirs
        val tempInputFile = new File(tempInput, "myrec.avro")
        val datumWriter = new SpecificDatumWriter[MyAvroRecord](classOf[MyAvroRecord]);
        val dataFileWriter = new DataFileWriter[MyAvroRecord](datumWriter)
        dataFileWriter.create(record.getSchema(), tempInputFile)
        dataFileWriter.append(record)
        dataFileWriter.close()
        val args = new Args(Map("input" -> List(tempInput.getPath), "output" -> List(tempOutput.getPath)))
        val mode = {
          val conf = new JobConf
          // Set the polling to a lower value to speed up tests:
          conf.set("jobclient.completion.poll.interval", "100")
          conf.set("cascading.flow.job.pollinginterval", "5")
          // Work around for local hadoop race
          conf.set("mapred.local.dir", "/tmp/hadoop/%s/mapred/local".format(java.util.UUID.randomUUID))
          Hdfs(true, conf)
        }
        val margs = Mode.putMode(mode, args) 

        val job = new SimpleUnpackedAvroJob(margs)
        val flow = job.buildFlow
        flow.complete

        val validationFile = io.Source.fromFile(tempOutput.getPath + "/part-00000").getLines
        res = validationFile.next.toDouble



        res == record.getPrice * 1.5
      }
      prop must pass
    }
  }
}

class AvroPackedSourceTest extends Specification with ScalaCheck {
  "Running M/R jobs on Avro data" should {
    "work for packed sources" in {
      val prop = Prop.forAll(MyAvroRecordGenerators.myAvroGen) { (record: MyAvroRecord) =>
        var res: Double = 0.0

        JobTest(classOf[SimplePackedAvroJob].getName)
          .arg("input", "inputFile")
          .arg("output", "outputFile")
          .source(PackedAvroSource[MyAvroRecord]("inputFile"), List(record))
          .sink[Double](Tsv("outputFile")) { outputBuffer =>
            res = outputBuffer(0)
          }
          .runHadoop
          .finish

        res == record.getPrice * 1.5
      }
      prop must pass
    }
  }
}
