---
layout: home
title:  "Home"
section: "home"
---

Scalding is a Scala library that makes it easy to specify Hadoop MapReduce jobs. Scalding is built on top of [Cascading](http://www.cascading.org/), a Java library that abstracts away low-level Hadoop details. Scalding is comparable to [Pig](http://pig.apache.org/), but offers tight integration with Scala, bringing advantages of Scala to your MapReduce jobs.

![Scalding Logo](https://raw.github.com/twitter/scalding/develop/logo/scalding.png)

### Word Count

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

The latest API docs are hosted at Scalding's [ScalaDoc index](api/).

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

## Get Involved + Code of Conduct

Pull requests and bug reports are always welcome!

Discussion occurs primarily on the Gitter channel: [![Chat](https://badges.gitter.im/twitter/scalding.svg)](https://gitter.im/twitter/scalding?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
Issues should be reported on the [GitHub issue tracker](https://github.com/twitter/scalding/issues).
Follow [@Scalding](http://twitter.com/scalding) on Twitter for updates.

We use a lightweight form of project governance inspired by the one used by Apache projects.

Please see [Contributing and Committership](https://github.com/twitter/analytics-infra-governance#contributing-and-committership) for our code of conduct and our pull request review process.

The TL;DR is send us a pull request, iterate on the feedback + discussion, and get a +1 from a [Committer](https://github.com/twitter/scalding/blob/develop/COMMITTERS.md) in order to get your PR accepted.

The current list of active committers (who can +1 a pull request) can be found here: [Committers](https://github.com/twitter/scalding/blob/develop/COMMITTERS.md)

A list of contributors to the project can be found here: [Contributors](https://github.com/twitter/scalding/graphs/contributors)

## License

Copyright 2016 Twitter, Inc.

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
