package com.twitter.scalding

import org.specs._
import cascading.flow.planner.PlannerException

class XHandlerTest extends Specification {

  "Throwable classes" should {
    "be handled if exist in default mapping" in {
      val rxh = RichXHandler()
      rxh.handlers.find(h => h(new PlannerException)).isDefined must beTrue
      rxh.handlers.find(h => h(new InvalidSourceException("Invalid Source"))).isDefined must beTrue
      rxh.handlers.find(h => h(new NoSuchMethodError)).isDefined must beTrue
      rxh.handlers.find(h => h(new AbstractMethodError)).isDefined must beTrue
      rxh.handlers.find(h => h(new NoClassDefFoundError)).isDefined must beTrue
    }
    "be handled if exist in custom mapping" in {
      val cRxh = RichXHandler(RichXHandler.mapping ++ Map(classOf[NullPointerException] -> "NPE"))
      cRxh.handlers.find(h => h(new NullPointerException)).isDefined must beTrue
      cRxh.mapping(classOf[NullPointerException]) must_== "NPE"
    }
    "not be handled if missing in mapping" in {
      val rxh = RichXHandler()
      rxh.handlers.find(h => h(new NullPointerException)).isDefined must beFalse
      rxh.handlers.find(h => h(new IndexOutOfBoundsException)).isDefined must beFalse
    }
    "be valid keys in mapping if defined" in {
      val rxh = RichXHandler()
      rxh.mapping(classOf[PlannerException]) must_== RichXHandler.RequireSinks
      rxh.mapping(classOf[InvalidSourceException]) must_== RichXHandler.DataIsMissing
      rxh.mapping(classOf[NoSuchMethodError]) must_== RichXHandler.BinaryProblem
      rxh.mapping(classOf[AbstractMethodError]) must_== RichXHandler.BinaryProblem
      rxh.mapping(classOf[NoClassDefFoundError]) must_== RichXHandler.BinaryProblem
      rxh.mapping(classOf[NullPointerException]) must_== RichXHandler.Default
    }
    "create a URL link in GitHub wiki" in {
      val NoClassDefFoundErrorString = "javalangnoclassdeffounderror"
      val AbstractMethodErrorString = "javalangabstractmethoderror"
      val NoSuchMethodErrorString = "javalangnosuchmethoderror"
      val InvalidSouceExceptionString = "comtwitterscaldinginvalidsourceexception"
      val PlannerExceptionString = "cascadingflowplannerplannerexception"
      RichXHandler.createXUrl(new PlannerException) must_== RichXHandler.gitHubUrl + PlannerExceptionString
      RichXHandler.createXUrl(new InvalidSourceException("Invalid Source")) must_== RichXHandler.gitHubUrl + InvalidSouceExceptionString
      RichXHandler.createXUrl(new NoSuchMethodError) must_== RichXHandler.gitHubUrl + NoSuchMethodErrorString
      RichXHandler.createXUrl(new AbstractMethodError) must_== RichXHandler.gitHubUrl + AbstractMethodErrorString
      RichXHandler.createXUrl(new NoClassDefFoundError) must_== RichXHandler.gitHubUrl + NoClassDefFoundErrorString
    }

  }
}
