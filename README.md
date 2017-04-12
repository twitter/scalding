# Scalding

[![Build status](https://img.shields.io/travis/twitter/scalding/develop.svg)](http://travis-ci.org/twitter/scalding)
[![Coverage Status](https://coveralls.io/repos/twitter/scalding/badge.png?branch=develop)](https://coveralls.io/r/twitter/scalding?branch=develop)
[![Latest version](https://index.scala-lang.org/twitter/scalding/scalding-core/latest.svg?color=orange)](https://index.scala-lang.org/twitter/scalding/scalding-core)
[![Chat](https://badges.gitter.im/twitter/scalding.svg)](https://gitter.im/twitter/scalding?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Scalding is a Scala library that makes it easy to specify Hadoop MapReduce jobs. Scalding is built on top of [Cascading](http://www.cascading.org/), a Java library that abstracts away low-level Hadoop details. Scalding is comparable to [Pig](http://pig.apache.org/), but offers tight integration with Scala, bringing advantages of Scala to your MapReduce jobs.

![Scalding Logo](https://raw.github.com/twitter/scalding/develop/logo/scalding.png)

## Word Count

Hadoop is a distributed system for counting words. Here is how it's done in Scalding.

```scala
package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.source.TypedText

class WordCountJob(args: Args) extends Job(args) {
  TypedPipe.from(TextLine(args("input")))
    .flatMap { line => tokenize(line) }
    .groupBy { word => word } // use each word for a key
    .size // in each group, get the size
    .write(TypedText.tsv[(String, Long)](args("output")))

  // Split a piece of text into individual words.
  def tokenize(text: String): Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
```

Notice that the `tokenize` function, which is standard Scala, integrates naturally with the rest of the MapReduce job. This is a very powerful feature of Scalding. (Compare it to the use of UDFs in Pig.)

You can find more example code under [examples/](https://github.com/twitter/scalding/tree/master/scalding-commons/src/main/scala/com/twitter/scalding/examples). If you're interested in comparing Scalding to other languages, see our [Rosetta Code page](https://github.com/twitter/scalding/wiki/Rosetta-Code), which has several MapReduce tasks in Scalding and other frameworks (e.g., Pig and Hadoop Streaming).

## Documentation and Getting Started

* [**Getting Started**](https://github.com/twitter/scalding/wiki/Getting-Started) page on the [Scalding Wiki](https://github.com/twitter/scalding/wiki)
* [Scalding Scaladocs](http://twitter.github.com/scalding) provide details beyond the API References. Prefer using this as it's always up to date.
* [**REPL in Wonderland**](tutorial/WONDERLAND.md) a hands-on tour of the scalding REPL requiring only git and java installed.
* [**Runnable tutorials**](https://github.com/twitter/scalding/tree/master/tutorial) in the source.
* The API Reference, including many example Scalding snippets:
  * [Type-safe API Reference](https://github.com/twitter/scalding/wiki/Type-safe-api-reference)
  * [Fields-based API Reference](https://github.com/twitter/scalding/wiki/Fields-based-API-Reference)
* The Matrix Library provides a way of working with key-attribute-value scalding pipes:
  * The [Introduction to Matrix Library](https://github.com/twitter/scalding/wiki/Introduction-to-Matrix-Library) contains an overview and a "getting started" example
  * The [Matrix API Reference](https://github.com/twitter/scalding/wiki/Matrix-API-Reference) contains the Matrix Library API reference with examples
* [**Introduction to Scalding Execution**](https://github.com/twitter/scalding/wiki/Calling-Scalding-from-inside-your-application) contains general rules and examples of calling Scalding from inside another application.

Please feel free to use the beautiful [Scalding logo](https://drive.google.com/folderview?id=0B3i3pDi3yVgNbm9pMUdDcHFKVEk&usp=sharing) artwork anywhere.

## Contact
For user questions or scalding development (internals, extending, release planning):
<https://groups.google.com/forum/#!forum/scalding-dev> (Google search also works as a first step)

In the remote possibility that there exist bugs in this code, please report them to:
<https://github.com/twitter/scalding/issues>

Follow [@Scalding](http://twitter.com/scalding) on Twitter for updates.

Chat: [![Gitter](https://badges.gitter.im/twitter/scalding.svg)](https://gitter.im/twitter/scalding?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

## Get Involved + Code of Conduct
Pull requests and bug reports are always welcome!

We use a lightweight form of project governence inspired by the one used by Apache projects.
Please see [Contributing and Committership](https://github.com/twitter/analytics-infra-governance#contributing-and-committership) for our code of conduct and our pull request review process.
The TL;DR is send us a pull request, iterate on the feedback + discussion, and get a +1 from a [Committer](COMMITTERS.md) in order to get your PR accepted.

The current list of active committers (who can +1 a pull request) can be found here: [Committers](COMMITTERS.md)

A list of contributors to the project can be found here: [Contributors](https://github.com/twitter/scalding/graphs/contributors)

## Building
There is a script (called sbt) in the root that loads the correct sbt version to build:

1. ```./sbt update``` (takes 2 minutes or more)
2. ```./sbt test```
3. ```./sbt assembly``` (needed to make the jar used by the scald.rb script)

The test suite takes a while to run. When you're in sbt, here's a shortcut to run just one test:

```> test-only com.twitter.scalding.FileSourceTest```

Please refer to [FAQ page](https://github.com/twitter/scalding/wiki/Frequently-asked-questions#issues-with-sbt) if you encounter problems when using sbt.

We use [Travis CI](http://travis-ci.org/) to verify the build:
[![Build Status](https://travis-ci.org/twitter/scalding.svg?branch=develop)](http://travis-ci.org/twitter/scalding)

We use [Coveralls](https://coveralls.io/r/twitter/scalding) for code coverage results:
[![Coverage Status](https://coveralls.io/repos/twitter/scalding/badge.png?branch=develop)](https://coveralls.io/r/twitter/scalding?branch=develop)

Scalding modules are available from maven central.

The current groupid and version for all modules is, respectively, `"com.twitter"` and  `0.17.0`.

Current published artifacts are

* `scalding-core_2.11`, `scalding-core_2.12`
* `scalding-args_2.11`, `scalding-args_2.12`
* `scalding-date_2.11`, `scalding-date_2.12`
* `scalding-commons_2.11`, `scalding-commons_2.12`
* `scalding-avro_2.11`, `scalding-avro_2.12`
* `scalding-parquet_2.11`, `scalding-parquet_2.12`
* `scalding-repl_2.11`, `scalding-repl_2.12`


The suffix denotes the scala version.

## Adopters

* Ebay
* Etsy
* Sharethrough
* Snowplow Analytics
* Soundcloud
* Twitter

To see a full list of users or to add yourself, see the [wiki](https://github.com/twitter/scalding/wiki/Powered-By)

## Authors:
* Avi Bryant <http://twitter.com/avibryant>
* Oscar Boykin <http://twitter.com/posco>
* Argyris Zymnis <http://twitter.com/argyris>

Thanks for assistance and contributions:

* Sam Ritchie <http://twitter.com/sritchie>
* Aaron Siegel: <http://twitter.com/asiegel>
* Ian O'Connell <http://twitter.com/0x138>
* Alex Levenson <http://twitter.com/THISWILLWORK>
* Jonathan Coveney <http://twitter.com/jco>
* Kevin Lin <http://twitter.com/reconditesea>
* Brad Greenlee: <http://twitter.com/bgreenlee>
* Edwin Chen <http://twitter.com/edchedch>
* Arkajit Dey: <http://twitter.com/arkajit>
* Krishnan Raman: <http://twitter.com/dxbydt_jasq>
* Flavian Vasile <http://twitter.com/flavianv>
* Chris Wensel <http://twitter.com/cwensel>
* Ning Liang <http://twitter.com/ningliang>
* Dmitriy Ryaboy <http://twitter.com/squarecog>
* Dong Wang <http://twitter.com/dongwang218>
* Josh Attenberg <http://twitter.com/jattenberg>
* Juliet Hougland <https://twitter.com/j_houg>
* Eddie Xie <https://twitter.com/eddiex>

A full list of [contributors](https://github.com/twitter/scalding/graphs/contributors) can be found on GitHub.

## License

Copyright 2016 Twitter, Inc.

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
