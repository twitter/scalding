# Scalding

Scalding is a Scala library that makes it easy to specify Hadoop MapReduce jobs. Scalding is built on top of [Cascading](http://www.cascading.org/), a Java library that abstracts away low-level Hadoop details. Scalding is comparable to [Pig](http://pig.apache.org/), but offers tight integration with Scala, bringing advantages of Scala to your MapReduce jobs.

![Scalding Logo](https://raw.github.com/twitter/scalding/develop/logo/scalding.png)

Current version: `0.12.0rc4`

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

You can find more example code under [examples/](https://github.com/twitter/scalding/tree/master/scalding-core/src/main/scala/com/twitter/scalding/examples). If you're interested in comparing Scalding to other languages, see our [Rosetta Code page](https://github.com/twitter/scalding/wiki/Rosetta-Code), which has several MapReduce tasks in Scalding and other frameworks (e.g., Pig and Hadoop Streaming).

## Documentation and Getting Started

* [**Getting Started**](https://github.com/twitter/scalding/wiki/Getting-Started) page on the [Scalding Wiki](https://github.com/twitter/scalding/wiki)
* [**Runnable tutorials**](https://github.com/twitter/scalding/tree/master/tutorial) in the source.
* The API Reference, including many example Scalding snippets:
  * [Fields-based API Reference](https://github.com/twitter/scalding/wiki/Fields-based-API-Reference)
  * [Type-safe API Reference](https://github.com/twitter/scalding/wiki/Type-safe-api-reference)
* [Scalding Scaladocs](http://twitter.github.com/scalding) provide details beyond the API References
* The Matrix Library provides a way of working with key-attribute-value scalding pipes:
  * The [Introduction to Matrix Library](https://github.com/twitter/scalding/wiki/Introduction-to-Matrix-Library) contains an overview and a "getting started" example
  * The [Matrix API Reference](https://github.com/twitter/scalding/wiki/Matrix-API-Reference) contains the Matrix Library API reference with examples

Please feel free to use the beautiful [Scalding logo](https://drive.google.com/folderview?id=0B3i3pDi3yVgNbm9pMUdDcHFKVEk&usp=sharing) artwork anywhere.

## Building
There is a script (called sbt) in the root that loads the correct sbt version to build:

1. ```./sbt update``` (takes 2 minutes or more)
2. ```./sbt test```
3. ```./sbt assembly``` (needed to make the jar used by the scald.rb script)

The test suite takes a while to run. When you're in sbt, here's a shortcut to run just one test:

```> test-only com.twitter.scalding.FileSourceTest```

Please refer to [FAQ page](https://github.com/twitter/scalding/wiki/Frequently-asked-questions#issues-with-sbt) if you encounter problems when using sbt.

We use [Travis CI](http://travis-ci.org/) to verify the build:
[![Build Status](https://secure.travis-ci.org/twitter/scalding.png)](http://travis-ci.org/twitter/scalding)

Scalding modules are available from maven central.

The current groupid and version for all modules is, respectively, `"com.twitter"` and  `0.11.0`.

Current published artifacts are

* `scalding-core_2.9.3`
* `scalding-core_2.10`
* `scalding-args_2.9.3`
* `scalding-args_2.10`
* `scalding-date_2.9.3`
* `scalding-date_2.10`
* `scalding-commons_2.9.3`
* `scalding-commons_2.10`
* `scalding-avro_2.9.3`
* `scalding-avro_2.10`
* `scalding-parquet_2.9.3`
* `scalding-parquet_2.10`
* `scalding-repl_2.9.3`
* `scalding-repl_2.10`


The suffix denotes the scala version.

## Adopters

* Ebay
* Etsy
* Sharethrough
* Snowplow Analytics
* Soundcloud
* Twitter

To see a full list of users or to add yourself, see the [wiki](https://github.com/twitter/scalding/wiki/Powered-By)

## Contact

For user questions, we are using the cascading-user mailing list for discussions:
<http://groups.google.com/group/cascading-user>

For scalding development (internals, extending, release planning):
<https://groups.google.com/forum/#!forum/scalding-dev>

In the remote possibility that there exist bugs in this code, please report them to:
<https://github.com/twitter/scalding/issues>

Follow [@Scalding](http://twitter.com/scalding) on Twitter for updates.

Chat (IRC): [freenode](https://webchat.freenode.net/) channel: #scalding

## Authors:
* Avi Bryant <http://twitter.com/avibryant>
* Oscar Boykin <http://twitter.com/posco>
* Argyris Zymnis <http://twitter.com/argyris>

Thanks for assistance and contributions:

* Sam Ritchie <http://twitter.com/sritchie>
* Aaron Siegel: <http://twitter.com/asiegel>
* Brad Greenlee: <http://twitter.com/bgreenlee>
* Edwin Chen <http://twitter.com/edchedch>
* Arkajit Dey: <http://twitter.com/arkajit>
* Krishnan Raman: <http://twitter.com/dxbydt_jasq>
* Flavian Vasile <http://twitter.com/flavianv>
* Chris Wensel <http://twitter.com/cwensel>
* Ning Liang <http://twitter.com/ningliang>
* Dmitriy Ryaboy <http://twitter.com/squarecog>
* Dong Wang <http://twitter.com/dongwang218>
* Kevin Lin <http://twitter.com/reconditesea>
* Josh Attenberg <http://twitter.com/jattenberg>
* Juliet Hougland <https://twitter.com/j_houg>

A full list of [contributors](https://github.com/twitter/scalding/graphs/contributors) can be found on GitHub.

## License
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
