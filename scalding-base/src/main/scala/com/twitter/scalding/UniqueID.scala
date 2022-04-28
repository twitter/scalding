package com.twitter.scalding

/**
 * Used to inject a typed unique identifier to uniquely name each scalding flow. This is here mostly to deal
 * with the case of testing where there are many concurrent threads running Flows. Users should never have to
 * worry about these
 */
case class UniqueID(get: String) {
  require(get.indexOf(',') == -1, s"UniqueID cannot contain ,: $get")
}

object UniqueID {
  val UNIQUE_JOB_ID = "scalding.job.uniqueId"
  private val id = new java.util.concurrent.atomic.AtomicInteger(0)

  def getRandom: UniqueID = {
    // This number is unique as long as we don't create more than 10^6 per milli
    // across separate jobs. which seems very unlikely.
    val unique = (System.currentTimeMillis << 20) ^ (id.getAndIncrement.toLong)
    UniqueID(unique.toString)
  }

  /**
   * This is only safe if you use something known to have a single instance in the relevant scope.
   *
   * In cascading, the FlowDef has been used here
   */
  def fromSystemHashCode(ar: AnyRef): UniqueID =
    UniqueID(System.identityHashCode(ar).toString)
}
