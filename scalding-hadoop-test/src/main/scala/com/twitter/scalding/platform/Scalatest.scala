/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.scalding.platform

import org.scalatest.{ BeforeAndAfterEach, Suite }

/**
 * This is a mixin fixture for scalatest which makes it easy to use a LocalCluster and will manage
 * the lifecycle of one appropriately.
 */
trait HadoopPlatformTest extends BeforeAndAfterEach { this: Suite =>
  org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.apache.hadoop.metrics2.util").setLevel(org.apache.log4j.Level.ERROR)

  val cluster = LocalCluster()

  def initialize() = cluster.initialize()

  override def beforeEach(): Unit = {
    cluster.synchronized {
      initialize()
    }
    super.beforeEach()
  }

  //TODO is there a way to buffer such that we see test results AFTER afterEach? Otherwise the results
  // get lost in the logging
  override def afterEach(): Unit = {
    try super.afterEach()
    finally {
      // Necessary because afterAll can be called from a different thread and we want to make sure that the state
      // is visible. Note that this assumes there is no contention for LocalCluster (which LocalCluster ensures),
      // otherwise there could be deadlock.
      cluster.synchronized {
        cluster.shutdown()
      }
    }
  }
}
