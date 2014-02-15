package com.twitter.scalding.avro

import com.twitter.scalding._

import org.specs.Specification
import org.specs.ScalaCheck
import org.scalacheck.Prop

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
      record
    }
    .write(Tsv(args("output")))
}

/** Illustrates how to unit test Avro jobs */
class AvroUnpackedSourceTest extends Specification with ScalaCheck {
  "Running M/R jobs on Avro data" should {
    "work for unpacked sources" in {
      val prop = Prop.forAll(MyAvroRecordGenerators.myAvroGen) { (record: MyAvroRecord) =>
        var res: Double = 0.0

        JobTest(classOf[SimpleUnpackedAvroJob].getName)
          .arg("input", "inputFile")
          .arg("output", "outputFile")
          .source(UnpackedAvroSource("inputFile", MyAvroRecord.SCHEMA$), List(record))
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
