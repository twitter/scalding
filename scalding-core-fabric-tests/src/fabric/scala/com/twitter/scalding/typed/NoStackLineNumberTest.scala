/*
Copyright 2015 Twitter, Inc.

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
package com.twitter.scalding.typed

/* moved from scalding-core due to a chicken-and-egg situation */

import cascading.flow.FlowDef
import com.twitter.scalding._
import org.apache.hadoop.conf.Configuration
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ Duration => SDuration }
import scala.concurrent.{ ExecutionContext => SExecutionContext, _ }
import Dsl._

class NoStackLineNumberTest extends WordSpec {

  "No Stack Shouldn't block getting line number info" should {
    "actually get the no stack info" in {
      implicit val fd = new FlowDef
      implicit val m = Mode.test("anyCluster-test", new Configuration, (_: Source) => None)

      val pipeFut = com.twitter.example.scalding.typed.InAnotherPackage.buildF.map { tp =>
        tp.toPipe('a, 'b)
      }
      val pipe = Await.result(pipeFut, SDuration.Inf)
      // We pick up line number info via the NoStackAndThenClass
      // So this should have some non-scalding info in it.
      assert(RichPipe.getPipeDescriptions(pipe).size > 0)
    }
  }
}