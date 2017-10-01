package com.twitter.scalding.some.other.space.space

sealed trait ContainerX
object ContainerX {
  case class ElementY(x: String) extends ContainerX
  case class ElementZ(x: String) extends ContainerX
}

// This is intentionally not sealed. User can supply their own
trait ContainerP {
  def id: String
}
object ContainerP {
  case object ElementA extends ContainerP {
    def id: String = "a"
  }
  case object ElementB extends ContainerP {
    def id: String = "b"
  }
  def fromId(id: String): ContainerP = id match {
    case _ if id == ElementA.id => ElementA
    case _ if id == ElementB.id => ElementB
  }
}

case class TestCaseHardA(e: ContainerX, y: String)
case class TestCaseHardB(e: ContainerP, y: String)
