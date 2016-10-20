package com.twitter.scalding.typed

import org.scalatest.Suites

class TypedFabricTests extends Suites(new NoStackLineNumberTest, new PartitionedDelimitedTest, new PartitionedTextLineTest)

