package com.twitter.scalding

import org.specs._

class Job1(args : Args) extends Job(args) {
  Tsv("in1").read.write(Tsv("output1"))
}

class Job2(args : Args) extends Job(args) {
  Tsv("in2").read.write(Tsv("output2"))
}

class CascadeJobTest extends Specification with TupleConversions {
  "Instantiating a CascadeJob" should {
    val newJobs = List(new Job1(Args("")), new Job2(Args("")))
    
    "add jobs via arguments" in {
      val cascadeJob = new CascadeJob(Args("--jobs com.twitter.scalding.Job1 com.twitter.scalding.Job2"))
      cascadeJob.jobs.map(_.name) must be_==(List("com.twitter.scalding.Job1","com.twitter.scalding.Job2"))
    }

    "add jobs via factory method" in {
      val cascadeJob = CascadeJob(newJobs:_*)
      cascadeJob.jobs.map(_.name) must be_==(List("com.twitter.scalding.Job1","com.twitter.scalding.Job2"))
    }
  }

  // "Running a CascadeJob" should {

  //   JobTest("com.twitter.scalding.CascadeJob")
  //     .arg("jobs", List("com.twitter.scalding.Job1", "com.twitter.scalding.Job2"))
  //     .run
  //     .finish

    
  // }
}
