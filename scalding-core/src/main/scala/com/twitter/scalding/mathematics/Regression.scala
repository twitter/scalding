package com.twitter.scalding.mathematics
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import cascading.flow.FlowDef
import cascading.pipe.Pipe

/**
Ordinary Least Squares
See: http://en.wikipedia.org/wiki/Ordinary_least_squares

@author : Krishnan Raman, kraman@twitter.com
*/

object Regression {

  // Ordinary Least Squares
  // Result: A Pipe with one row, two columns alpha & beta
  //  Y = beta * X + alpha
  // Data in columns x & y will be scaled and normalized before OLS fit
  def ols(p:RichPipe, x:Symbol, y:Symbol)(implicit flowDef: FlowDef, mode: Mode): Pipe = {

    def ndigits(x:Double, n:Int) = math.round(x * math.pow(10,n)).toInt/math.pow(10,n)

    // scale column c so variates remapped to unit interval
    // augment pipe with 3 columns, cmin, cscaled, cscaler, given symbol c
    def scale(p:RichPipe, c:Symbol) :RichPipe = {
      val xt = ('xmin, 'xmax, 'xscaler, 'xscaled)
      val yt = ('ymin, 'ymax, 'yscaler, 'yscaled)
      val (mins, maxs, scalers, scaleds) = if (c.toString.startsWith("'x")) xt else yt

      val minmax = p.groupAll {
        _.min(c -> mins)
         .max(c -> maxs)
      }

      p.crossWithTiny(minmax)
      .map((c, mins, maxs) -> (scaleds, scalers)) {
        x:(Double, Double, Double) =>
        val (data, min, max) = x
        val scaler = max - min
        ((data - min)/scaler, scaler)
      }
      .discard(maxs)
    }

    // normalize column c so variates have zero mean, unit variance
    // augment with 3 columns, cavg, cstdev, cnorm, given column c
    def normalize(p:RichPipe, c:Symbol): RichPipe = {
      val xt = ('xavg, 'xstdev, 'xnorm)
      val yt = ('yavg, 'ystdev, 'ynorm)
      val (avgs, stdevs, norms) = if (c.toString.startsWith("'x")) xt else yt

      val stats = p.groupAll {
        _.sizeAveStdev(c -> ('n__, avgs, stdevs))
      }
      .discard('n__)

      p.crossWithTiny(stats)
      .map((c, avgs, stdevs) -> norms) {
        x:(Double, Double, Double) =>
        val (data, avg, stdev) = x
        (data-avg)/stdev
      }
    }

    val pipe = p.project(x,y).rename(x -> 'x).rename(y ->'y)

    val normpipe = normalize( normalize( scale( scale(pipe, 'x), 'y), 'xscaled), 'yscaled)

    val corrpipe = normpipe.map(('xnorm,'ynorm) -> 'xy){
      z:(Double, Double) =>
      val (x,y) = z
      x*y
    }.groupAll {
      _.average('xy -> 'corr)
    }

    val tuples = ('xmin, 'xscaler, 'xavg, 'xstdev, 'xnorm,
                  'ymin, 'yscaler, 'yavg, 'ystdev, 'ynorm,
                  'corr)

    normpipe
    .crossWithTiny(corrpipe)
    .limit(1)
    .project(tuples)
    .mapTo(tuples -> ('alpha, 'beta)){
      t:(Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double) =>
      val (xmin, xscaler, xavg, xstdev, xnorm, ymin,yscaler,yavg,ystdev,ynorm,corr) = t
      val beta = (ystdev * corr * yscaler)/(xstdev * xscaler)
      val alpha = ymin + yscaler * (yavg - (ystdev * corr / xstdev ) * (xmin / xscaler + xavg))
      (ndigits(alpha, 3), ndigits(beta,3))
    }
  }
}
