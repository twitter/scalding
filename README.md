# Scalding

Scalding is a Scala library that makes it easy to specify Hadoop MapReduce jobs. Scalding is built on top of [Cascading](http://www.cascading.org/), a Java library that abstracts away low-level Hadoop details. Scalding is comparable to [Pig](http://pig.apache.org/), but offers tight intergation with Scala, bringing advantages of Scala to your MapReduce jobs.

Current version: 0.8.2

## Word Count

Hadoop is a distributed system for counting words. Here is how it's done in Scalding.

```scala
package com.twitter.scalding.examples

import com.twitter.scalding._

class WordCountJob(args : Args) extends Job(args) {
  TextLine( args("input") )
    .flatMap('line -> 'word) { line : String => tokenize(line) }
    .groupBy('word) { _.size }
    .write( Tsv( args("output") ) )

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
```

Notice that the `tokenize` function, which is standard Scala, integrates naturally with the rest of the MapReduce job. This is a very powerful feature of Scalding. (Compare it to the use of UDFs in Pig.)

You can find more example code under [examples/](https://github.com/twitter/scalding/tree/master/src/main/scala/com/twitter/scalding/examples). If you're interested in comparing Scalding to other languages, see our [Rosetta Code page](https://github.com/twitter/scalding/wiki/Rosetta-Code), which has several MapReduce tasks in Scalding and other frameworks (e.g., Pig and Hadoop Streaming).

## Documentation and Getting Started

* [**Getting Started**](https://github.com/twitter/scalding/wiki/Getting-Started) page on the [Scalding Wiki](https://github.com/twitter/scalding/wiki)
* [**Runnable tutorials**](https://github.com/twitter/scalding/tree/master/tutorial) in the source.
* The API Reference, including many example Scalding snippets:
  * [Fields-based API Reference](https://github.com/twitter/scalding/wiki/Fields-based-API-Reference)
  * [Type-safe API Reference](https://github.com/twitter/scalding/wiki/Type-safe-api-reference)
* [Scalding Scaladocs](http://twitter.github.com/scalding/target/scala-2.9.2/api) provide details beyond the API References
* The Matrix Library provides a way of working with key-attribute-value scalding pipes:
  * The [Introduction to Matrix Library](https://github.com/twitter/scalding/wiki/Introduction-to-Matrix-Library) contains an overview and a "getting started" example 
  * The [Matrix API Reference](https://github.com/twitter/scalding/wiki/Matrix-API-Reference) contains the Matrix Library API reference with examples

## Building
0. Install [sbt 0.11.3](http://scalasbt.artifactoryonline.com/scalasbt/sbt-native-packages/org/scala-sbt/sbt-launcher/0.11.3/) (sorry, but the assembly plugin is sbt version dependent).
1. ```sbt update``` (takes 2 minutes or more)
2. ```sbt test```
3. ```sbt assembly``` (needed to make the jar used by the scald.rb script)

The test suite takes a while to run. When you're in sbt, here's a shortcut to run just one test:

```> test-only com.twitter.scalding.FileSourceTest```

We use [Travis CI](http://travis-ci.org/) to verify the build:
[![Build Status](https://secure.travis-ci.org/twitter/scalding.png)](http://travis-ci.org/twitter/scalding)

The current version is 0.8.2 and is available from maven central: org="com.twitter", artifact="scalding_2.9.2".

## Contact

Currently we are using the cascading-user mailing list for discussions:
<http://groups.google.com/group/cascading-user>

In the remote possibility that there exist bugs in this code, please report them to:
<https://github.com/twitter/scalding/issues>

Follow [@Scalding](http://twitter.com/scalding) on Twitter for updates.

## Authors:
* Avi Bryant <http://twitter.com/avibryant>
* Oscar Boykin <http://twitter.com/posco>
* Argyris Zymnis <http://twitter.com/argyris>

Thanks for assistance and contributions:

* Chris Wensel <http://twitter.com/cwensel>
* Ning Liang <http://twitter.com/ningliang>
* Dmitriy Ryaboy <http://twitter.com/squarecog>
* Dong Wang <http://twitter.com/dongwang218>
* Edwin Chen <http://twitter.com/edchedch>
* Sam Ritchie <http://twitter.com/sritchie09>
* Flavian Vasile <http://twitter.com/flavianv>

## License
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
