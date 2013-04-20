package com.twitter.scalding

import org.specs._

class XHandlerTest extends Specification {

  "Throwable classes" should {
    "be handled if exist in default mapping" in {
      val rxh = RichXHandler()
      rxh.handlers.find(h => h(new IndexOutOfBoundsException)).isDefined must beTrue
      rxh.handlers.find(h => h(new AbstractMethodError)).isDefined must beTrue
      rxh.handlers.find(h => h(new NoClassDefFoundError)).isDefined must beTrue
    }
    "be handled if exist in custom mapping" in {
      val crxh = RichXHandler(RichXHandler.mapping ++ Map(classOf[NullPointerException] -> "NPE"))
      crxh.handlers.find(h => h(new NullPointerException)).isDefined must beTrue
      crxh.mapping(classOf[NullPointerException]) must_== "NPE"
    }
    "not be handled if missing in mapping" in {
      val rxh = RichXHandler()
      rxh.handlers.find(h => h(new NullPointerException)).isDefined must beFalse
      rxh.handlers.find(h => h(new StringIndexOutOfBoundsException)).isDefined must beFalse
    }
    "be valid keys in mapping" in {
      val rxh = RichXHandler()
      rxh.mapping(classOf[IndexOutOfBoundsException]) must_== RichXHandler.RequireAllSources
      rxh.mapping(classOf[NoClassDefFoundError]) must_== RichXHandler.BinaryProblem
      rxh.mapping(classOf[AbstractMethodError]) must_== RichXHandler.DataIsMissing
      rxh.mapping(classOf[NullPointerException]) must_== RichXHandler.Default
    }

  }
}
