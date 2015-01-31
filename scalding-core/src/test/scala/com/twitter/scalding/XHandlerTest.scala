/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }
import cascading.flow.planner.PlannerException

class XHandlerTest extends WordSpec with Matchers {

  "Throwable classes" should {
    "be handled if exist in default mapping" in {
      val rxh = RichXHandler()
      rxh.handlers.find(h => h(new PlannerException)) should not be empty
      rxh.handlers.find(h => h(new InvalidSourceException("Invalid Source"))) should not be empty
      rxh.handlers.find(h => h(new NoSuchMethodError)) should not be empty
      rxh.handlers.find(h => h(new AbstractMethodError)) should not be empty
      rxh.handlers.find(h => h(new NoClassDefFoundError)) should not be empty
    }
    "be handled if exist in custom mapping" in {
      val cRxh = RichXHandler(RichXHandler.mapping ++ Map(classOf[NullPointerException] -> "NPE"))
      cRxh.handlers.find(h => h(new NullPointerException)) should not be empty
      cRxh.mapping(classOf[NullPointerException]) shouldBe "NPE"
    }
    "not be handled if missing in mapping" in {
      val rxh = RichXHandler()
      rxh.handlers.find(h => h(new NullPointerException)) shouldBe empty
      rxh.handlers.find(h => h(new IndexOutOfBoundsException)) shouldBe empty
    }
    "be valid keys in mapping if defined" in {
      val rxh = RichXHandler()
      rxh.mapping(classOf[PlannerException]) shouldBe RichXHandler.RequireSinks
      rxh.mapping(classOf[InvalidSourceException]) shouldBe RichXHandler.DataIsMissing
      rxh.mapping(classOf[NoSuchMethodError]) shouldBe RichXHandler.BinaryProblem
      rxh.mapping(classOf[AbstractMethodError]) shouldBe RichXHandler.BinaryProblem
      rxh.mapping(classOf[NoClassDefFoundError]) shouldBe RichXHandler.BinaryProblem
      rxh.mapping(classOf[NullPointerException]) shouldBe RichXHandler.Default
    }
    "create a URL link in GitHub wiki" in {
      val NoClassDefFoundErrorString = "javalangnoclassdeffounderror"
      val AbstractMethodErrorString = "javalangabstractmethoderror"
      val NoSuchMethodErrorString = "javalangnosuchmethoderror"
      val InvalidSouceExceptionString = "comtwitterscaldinginvalidsourceexception"
      val PlannerExceptionString = "cascadingflowplannerplannerexception"
      RichXHandler.createXUrl(new PlannerException) shouldBe (RichXHandler.gitHubUrl + PlannerExceptionString)
      RichXHandler.createXUrl(new InvalidSourceException("Invalid Source")) shouldBe (RichXHandler.gitHubUrl + InvalidSouceExceptionString)
      RichXHandler.createXUrl(new NoSuchMethodError) shouldBe (RichXHandler.gitHubUrl + NoSuchMethodErrorString)
      RichXHandler.createXUrl(new AbstractMethodError) shouldBe (RichXHandler.gitHubUrl + AbstractMethodErrorString)
      RichXHandler.createXUrl(new NoClassDefFoundError) shouldBe (RichXHandler.gitHubUrl + NoClassDefFoundErrorString)
    }
  }
}
