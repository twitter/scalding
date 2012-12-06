package com.twitter.scalding.mathematics

import org.specs._
import com.twitter.scalding._

class CombinatoricsJob(args : Args) extends Job(args) {
  val C = Combinatorics
  C.permutations( 5,2 ).write(Tsv("perms.txt"))

  C.combinations( 5,2 ).write(Tsv("combs.txt"))

  // how many ways can you invest $10000 in KR,ABT,DLTR,MNST ?
  val cash = 1000.0
  val error = 1.0 // max error $1, so its ok if we cannot invest the last dollar
  val (kr,abt,dltr,mnst) = (27.0,64.0,41.0,52.0) // share prices
  val stocks = IndexedSeq( kr,abt,dltr,mnst)


  C.weightedSum( stocks, cash,error).write( Tsv("invest.txt"))
  C.positiveWeightedSum( stocks, cash,error).write( Tsv("investpos.txt"))

}

class CombinatoricsJobTest extends Specification {
  noDetailedDiffs()
  import Dsl._

  "A Combinatorics Job" should {
    JobTest( new CombinatoricsJob(_))
      .source()
      .sink(Tsv("perms.txt")) { buf =>
        val size = buf.toList.size
        "correctly compute 5 permute 2 equals 20" in {
          size must be_==20
        }
        .sink(Tsv("combs.txt")) { buf =>
        val size = buf.toList.size
        "correctly compute 5 choose 2 equals 10" in {
          size must be_==10
        }
        .sink(Tsv("invest.txt")) { buf =>
        val size = buf.toList.size
        "correctly compute 169 tuples that allow you to invest $1000 among the 4 given stocks" in {
          size must be_==169
        }
        .sink(Tsv("investpos.txt")) { buf =>
        val size = buf.toList.size
        "correctly compute 101 non-zero tuples that allow you to invest $1000 among the 4 given stocks" in {
          size must be_==101
        }
      .run
      .finish
  }
}
