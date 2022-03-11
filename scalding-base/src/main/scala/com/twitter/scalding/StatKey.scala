package com.twitter.scalding

case class StatKey(counter: String, group: String) extends java.io.Serializable

object StatKey {
  // This is implicit to allow Stat("c", "g") to work.
  implicit def fromCounterGroup(counterGroup: (String, String)): StatKey = counterGroup match {
    case (c, g) => StatKey(c, g)
  }
  // Create a Stat in the ScaldingGroup
  implicit def fromCounterDefaultGroup(counter: String): StatKey =
    StatKey(counter, ScaldingGroup)

  val ScaldingGroup = "Scalding Custom"
}