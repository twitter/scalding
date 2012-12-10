package com.twitter.scalding.mathematics

import org.specs._
import com.twitter.scalding._

class CombinatoricsJob(args : Args) extends Job(args) {
  val C = Combinatorics
  C.permutations( 10,3 ).write(Tsv("perms.txt"))

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
      .sink[(Int,Int)](Tsv("perms.txt")) { pbuf =>
        val psize = pbuf.toList.size
        "correctly compute 10 permute 3 equals 720" in {
          psize must be_==(720)
        }
      }
      .sink[(Int,Int)](Tsv("combs.txt")) { buf =>
        val csize = buf.toList.size
        "correctly compute 5 choose 2 equals 10" in {
          csize must be_==(10)
        }
      }
      .sink[(Int,Int,Int,Int)](Tsv("invest.txt")) { buf =>
        val isize = buf.toList.size
        "correctly compute 169 tuples that allow you to invest $1000 among the 4 given stocks" in {
          isize must be_==(169)
        }
      }
      .sink[(Int,Int,Int,Int)](Tsv("investpos.txt")) { buf =>
        val ipsize = buf.toList.size
        "correctly compute 101 non-zero tuples that allow you to invest $1000 among the 4 given stocks" in {
          ipsize must be_==(101)
        }
      }
      .run
      .finish
  }
}
