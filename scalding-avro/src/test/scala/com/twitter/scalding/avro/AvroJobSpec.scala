package com.twitter.scalding.avro

import _root_.avro.FiscalRecord
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding._
import com.twitter.scalding.typed.TDsl._
import org.specs.Specification

/**
 * @author Mansur Ashraf.
 */
object AvroJobSpec extends Specification {
  "AvroTestJob job" should {
    JobTest(a => new AvroTestJob(a)).
      arg("input", "input").
      arg("output", "output").
      source(Tsv("input"), List[(String, Int, Int)](("2014-10-01", 1, 2014))).
      sink[FiscalRecord](UnpackedAvroSource[FiscalRecord]("output")){ outputBuffer =>
        val result = outputBuffer.toList
        "count words correctly" in {
          result.size must_== 1
        }
      }.
      run.
      finish
  }
}

class AvroTestJob(args: Args) extends Job(args) {
  TypedPipe.from[(String, Int, Int)](Tsv(args("input")), List("a", "b", "c"))
    .map{
      case (date, week, year) =>
        new FiscalRecord(date, week, year)
    }
    .write(UnpackedAvroSource[FiscalRecord](args("output")))

}
