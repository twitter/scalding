package com.twitter.scalding

import org.scalatest.Suites

class FabricTests extends Suites(new FileSourceHadoopExtraTest, new typed.TypedFabricTests)
