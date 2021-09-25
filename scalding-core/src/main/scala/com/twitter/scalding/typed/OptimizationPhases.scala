package com.twitter.scalding.typed

import com.stripe.dagon.Rule

/**
 * This is a class to allow customization
 * of how we plan typed pipes
 */
abstract class OptimizationPhases {
  def phases: Seq[Rule[TypedPipe]]
}

final class EmptyOptimizationPhases extends OptimizationPhases {
  def phases = Nil
}
