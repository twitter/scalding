package com.twitter.scalding.mathematics

import scala.util.Random

/**
 * Generating Poisson-distributed random variables
 * according to Donald Knuth's algorith as shown on Wikipedia's
 * Poisson Distribution page
 */

class Poisson(fraction: Double, seed: Int) {

  val L = math.exp(-fraction)
  val randomGenerator = new Random(seed)

  def nextInt = {
    var k = 0
    var p = 1.0
    do {
      k = k + 1
      p = p * randomGenerator.nextDouble
    } while (p > L)
    k - 1
  }
}