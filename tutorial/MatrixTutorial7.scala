package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics._
import com.twitter.scalding.mathematics.Matrix._

/*
* MatrixTutorial7.scala
*
* Typed API example on Matrix.  Loads in the movie ratings dataset and calculates/prints the correlation adjacency matrix.
*
* ../scripts/scald.rb --local MatrixTutorial7.scala --input data/docBOW.tsv --output data/sim.tsv
*
*/

// cosine distance and pearson correlation
// see http://ow.ly/vtm44
 
class SimJob(args : Args) extends Job(args) {
 
  val ratings = TypedTsv[(String, String, Double)]( args("input"), ('user, 'movie, 'rating) )
    .read
    .toMatrix[String,String,Double]('user, 'movie, 'rating)
 
  val mom1 = ratings.rowSizeAveStdev.getCol(1)
  val mom2 = ratings.rowSizeAveStdev.getCol(2)
  val mom3 = ratings.rowSizeAveStdev.getCol(3)
  //mom1.write()
  //mom2.write()
  //mom3.write()
 
  val medians = ratings.mapRows( median )
  val ratingD = ratings.elemWiseOp( medians )((x,y) => if (x>y) 1.0 else 0.0)
  //ratingD.write()
 
  val ratingN = ratings.rowL2Normalize
  val adjCos = ratingN * ratingN.transpose
  //adjCos.write()
 
  val ratingCN = ratings.rowMeanCentering.rowL2Normalize
  val adjCor = ratingCN * ratingCN.transpose
  adjCor.write( TextLine( args("output") ) )
 
  def median[T](vct: Iterable[(T,Double)]) : Iterable[(T,Double)] = {
    val vList = vct.toList.sortWith((t1,t2) => t1._2 < t2._2).toList
    val midpoint = (vList.size / 2).floor.toInt
    val median = vList(midpoint)
    vct.map { tup => (tup._1, median._2) }
  }
}
