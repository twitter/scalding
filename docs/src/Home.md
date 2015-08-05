Scalding is a Scala library that makes it easy to write MapReduce jobs in Hadoop. It's similar to other MapReduce platforms like Pig and Hive, but offers a higher level of abstraction by leveraging the full power of Scala and the JVM.

Scalding is built on top of [Cascading](http://www.cascading.org/), a Java library that abstracts away much of the complexity of Hadoop (such as the need to write raw `map` and `reduce` functions).

*Need a suggestion for where to start?* Try the [Alice in Wonderland walkthrough](https://gist.github.com/johnynek/a47699caa62f4f38a3e2) which shows how to use Scalding step by step to learn about the book's text.

## Getting help
* [Cascading Google Group](https://groups.google.com/forum/?fromgroups#!forum/cascading-user). We are using this Google Group for Scalding questions as well.
* [\@Scalding on Twitter](http://twitter.com/scalding)
* [IRC: #scalding on freenode](http://webchat.freenode.net/?channels=scalding)
* [[Frequently Asked Questions]]

## Documentation
* [Scaladocs](http://twitter.github.com/scalding): Generated documentation for current version of Scalding.
* Note: `sbt doc` will build scaladocs under the `target/2.9.2/api/` directory, which you can then open in your browser.
* Tutorials
  * Beginner
    * [[Getting Started]]
    * [[Scalding REPL]]: Learning is better when it's interactive. This tutorial shows off how to interact with your data using the Scalding REPL.
    * [Alice in Wonderland walkthrough](https://gist.github.com/johnynek/a47699caa62f4f38a3e2): Step-by-step example of using Scalding in Local mode in the REPL.
    * [[Intro to Scalding Jobs]]
  * Intermediate
    * [[Aggregation using Algebird Aggregators]]. Continuing the SQL analogy, we see how to use composable Aggregators.
    * [[SQL to Scalding]]. Canonical ways of translating common SQL idioms to Scalding.
  * Advanced
    * [[Building Bigger Platforms With Scalding]] some approaches for modular design and composing with scalding.
    * [Getting Started with the Matrix library](https://github.com/twitter/scalding/wiki/Introduction-to-Matrix-Library)
* Reference/Other
  * [[Type-safe API Reference]]. This API is very close to the scala collections API.
  * [[REPL Reference]]
  * [[Matrix-API-Reference]]
  * [[Scalding Sources]]
  * [[Scalding-Commons]]. The README of the former scalding-commons library.
  * [[Rosetta Code]]. A collection of MapReduce tasks translated (from Pig, Hive, Cascalog, MapReduce Streaming, etc.) into Scalding.
  * [Oscar's Scalding Talk at the Hadoop Summit](http://www.slideshare.net/johnynek/scalding). Slides from Oscar's talk at the Hadoop Summit.
  * [[Upgrading to 0.9.0]] means fixing some compile issues. These [sed rules](https://gist.github.com/johnynek/6632488) may help.
  * **DEPRECATED**: [[Fields-based API Reference]]. This is the original, Cascading DSL API to scalding using a named tuple model.
  We highly recommend the Type-safe API, using TypedPipe, for any new code. This page also contains many example code snippets illustrating each Scalding function.  See [[Field Rules]] for more on Fields.

## Third Party Modules
* [Scalding-cassandra](https://github.com/sonarme/scalding-cassandra) support for reading/writing cassandra
* [Spy Glass] (https://github.com/ParallelAI/SpyGlass) - Advanced featured HBase wrapper for Cascading and Scalding

## Videos
* [Scalding: Powerful & Concise MapReduce Programming](http://www.youtube.com/watch?v=LaAEhPoIm_A)
* [Scalding lecture for UC Berkeley's Analyzing Big Data with Twitter class](http://blogs.ischool.berkeley.edu/i290-abdt-s12/2012/11/03/video-lecture-intro-to-scalding-by-posco-and-argyris/)

## How-tos
* [[Scalding with CDH3U2 in a Maven project]]
* [Running your Scalding jobs in Eclipse](http://hokiesuns.blogspot.com/2012/07/running-your-scalding-jobs-in-eclipse.html)
* [Run/Test jobs locally from Intellij IDEA](https://github.com/twitter/scalding/wiki/Run-in-Intellij-IDEA)
* [Running your Scalding jobs in IDEA intellij](http://willwhim.wpengine.com/2013/02/28/using-intellij-with-twitters-scalding/)
* [Running Scalding jobs on EMR](https://github.com/snowplow/scalding-example-project)
* Running Scalding with HBase support: [[Scalding HBase]] wiki
* [Using the distributed cache](https://github.com/twitter/scalding/wiki/Using-the-distributed-cache)
* [[Calling Scalding from inside your application]]
* [Unit Testing Scalding Jobs](http://www.agileatwork.com/unit-testing-scalding-jobs/)
* [Using counters](https://github.com/tomer-ben-david-examples/scalding-counters-example)

## Tutorials

NOTE: all of the following tutorials use the Fields API, which is deprecated

* [Scalding for the impatient](http://sujitpal.blogspot.com/2012/08/scalding-for-impatient.html) great set of tutorials on using scalding walking through simple to more complex examples (including TF-IDF).
* [Movie Recommendations and more in MapReduce and Scalding](http://blog.echen.me/2012/02/09/movie-recommendations-and-more-via-mapreduce-and-scalding/)
* [Generating Recommendations with MapReduce and Scalding](http://engineering.twitter.com/2012/03/generating-recommendations-with.html), a shorter version of the above post.
* [Poker collusion detection with Mahout and Scalding](http://www.javacodegeeks.com/2012/08/mahout-and-scalding-for-poker-collusion.html)
* [Portfolio Management in Scalding](http://www.jasq.org/2/post/2012/12/portfolio-mgmt-in-scalding.html)
* [Find the Fastest Growing County in US, 1969-2011, using Scalding](https://gist.github.com/4696053)
* [Dean Wampler's Scalding Workshop](https://github.com/deanwampler/scalding-workshop). Presented by [Dean](https://twitter.com/deanwampler) at [StrangeLoop 2012](https://thestrangeloop.com/).
* [Typesafe's Activator for Scalding](http://typesafe.com/activator/template/activator-scalding). Also created by [Dean Wampler](https://twitter.com/deanwampler).

## Articles and presentations from around the web
* [Hive, Pig, Scalding, Scoobi, Scrunch and Spark: A Comparison of Hadoop Frameworks](http://blog.samibadawi.com/2012/03/hive-pig-scalding-scoobi-scrunch-and.html)
* [Why Hadoop MapReduce needs Scala](http://speakerdeck.com/u/agemooij/p/why-hadoop-mapreduce-needs-scala)
* [How Twitter is doing its part to democratize big data](http://gigaom.com/cloud/how-twitter-is-doing-its-part-to-democratize-big-data/)
* [Meet the combo powering Hadoop at Etsy, Airbnb and Climate Corp.](http://gigaom.com/data/meet-the-combo-behind-etsy-airbnb-and-climate-corp-hadoop-jobs/)
* [Scalding wins a Bossie award from InfoWorld](http://www.infoworld.com/slideshow/65089/bossie-awards-2012-the-best-open-source-databases-202354#slide3)
* [Scalding: Hadoop Word Count in LESS than 70 lines of code](http://www.slideshare.net/ktoso/scalding-hadoop-word-count-in-less-than-60-lines-of-code)

## Other
* [[Using Scalding with other versions of Scala]]
* [[Scala and sbt for Homebrew users]]
* [[Scala and sbt for MacPorts users]]
* [[Comparison to Scrunch and Scoobi]]
* [[Powered-By]] see who is using scalding in production.

## Documentation Todo
* [[scald.rb]]
* [[Reading/Writing Source and Sink Taps]]
* [[Scalding on Amazon Elastic MapReduce]]
* [[Brief Introduction to Cascading]]
