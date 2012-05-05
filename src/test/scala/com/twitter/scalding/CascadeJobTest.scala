package com.twitter.scalding

import org.specs._

class Job1(args : Args) extends Job(args) {
}

class Job2(args : Args) extends Job(args) {
}

class CascadeJobTest extends Specification with TupleConversions {
  "A CascadeJob" should {
    val newJobs = Array(new Job1(Args("")), new Job2(Args("")))
    
    "add jobs via addJobs (via addJob)" in {
      val cascadeJob = new CascadeJob(Args("")){
        addJobs(newJobs:_*)
      }
      cascadeJob.jobs must be_==(newJobs.toList.reverse)
    }

    "add jobs via reflective addJob" in {
      val cascadeJob = new CascadeJob(Args("")){
        addJob("com.twitter.scalding.Job1", Args("--hi ho"))
      }
      cascadeJob.jobs.head.name must be_==("com.twitter.scalding.Job1")
      cascadeJob.jobs.head.args("hi") must be_==("ho")
    }

    "add jobs via arguments" in {
      val cascadeJob = new CascadeJob(Args("--jobs com.twitter.scalding.Job1 com.twitter.scalding.Job2"))
      cascadeJob.jobs.map(_.name) must be_==(List("com.twitter.scalding.Job2","com.twitter.scalding.Job1"))
    }
  }
}
