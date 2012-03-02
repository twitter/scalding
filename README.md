# Scalding

Current version: 0.3.5

## Summary
Scalding is a library that has two components:

* a scala DSL to make map-reduce computations look very similar to scala's collection API
* a wrapper to Cascading to make simpler to define the usual use cases of jobs, tests and describing new data on HDFS.

To run scala scalding jobs, a script, scald.rb is provided in scripts/. Run this script
with no arguments to see usage tips.  You will need to customize the default variables
at the head of that script for your environment.

You should follow the scalding project on twitter: <http://twitter.com/scalding>

## Word Count
Hadoop is a distributed system for counting words.  Here is how it's done in scalding.  You can find this in examples:

```scala
package com.twitter.scalding.examples

import com.twitter.scalding._

class WordCountJob(args : Args) extends Job(args) {
  TextLine( args("input") ).read.
    flatMap('line -> 'word) { line : String => line.split("\\s+") }.
    groupBy('word) { _.size }.
    write( Tsv( args("output") ) )
}
```

##Tutorial
See tutorial/ for examples of how to use the DSL.  See tutorial/CodeSnippets.md for some
example scalding snippets. Edwin Chen wrote an excellent tutorial on using scalding for
recommendations:
<http://blog.echen.me/2012/02/09/movie-recommendations-and-more-via-mapreduce-and-scalding/>

## Building
0. Install sbt 0.11
1. ```sbt update``` (takes 2 minutes or more)
2. ```sbt test```
3. ```sbt assembly``` (needed to make the jar used by the scald.rb script)

We use Travis-ci.org to verify the build:
[![Build Status](https://secure.travis-ci.org/twitter/scalding.png)](http://travis-ci.org/twitter/scalding)

The current version is 0.3.5 and available from maven central: org="com.twitter", artifact="scalding_2.8.1".

## Comparison to Scrunch/Scoobi
Scalding comes with an executable tutorial set that does not require a Hadoop
cluster.  If you're curious about scalding, why not invest a bit of time and run the tutorial
yourself and make your own judgement.

Scalding was developed before either of those projects
were announced publicly and has been used in production at Twitter for more than six months
(though it has been through a few iterations internally).
The main difference between Scalding (and Cascading) and Scrunch/Scoobi is that Cascading has
a record model where each element in your distributed list/table is a table with some named
fields.  This is nice because most common cases are to have a few primitive columns (ints, strings,
etc...).  This is discussed in detail in the two answers to the following question:
<http://www.quora.com/Apache-Hadoop/What-are-the-differences-between-Crunch-and-Cascading>

Scoobi and Scrunch stress types and do not
use field names to build ad-hoc record types.  Cascading's fields are very convenient,
and our users have been very productive with Scalding. Fields do present problems for
type inference because Cascading cannot tell you the type of the data in Fields("user_id", "clicks")
at compile time.  This could be surmounted by building a record system in scala that
allows the programmer to express the types of the fields, but the cost of this is not trivial,
and the win is not so clear.

Scalding supports using any scala object in your map/reduce operations using Kryo serialization,
including scala Lists, Sets,
Maps, Tuples, etc.  It is not clear that such transparent serialization is present yet in
scrunch.  Like Scoobi, Scalding has a form of MSCR fusion by relying on Cascading's AggregateBy
operations.  Our Reduce primitives (see GroupBuilder.reduce and .mapReduceMap) are comparable to
Scoobi's combine primitive, which by default uses Hadoop combiners on the map side.

Lastly, Scalding comes with a script that allows you to write a single file and run that
single file locally or on your Hadoop cluster by typing one line "scald.rb [--local] myJob.scala".
It is really convenient to use the same language/tool to run jobs on Hadoop and then to post-process
the output locally.

## Mailing list

Currently we are using the cascading-user mailing list for discussions.
<http://groups.google.com/group/cascading-user>

Follow the scalding project on twitter for updates: <http://twitter.com/scalding>

## Bugs
In the remote possibility that there exist bugs in this code, please report them to:
<https://github.com/twitter/scalding/issues>

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

## License
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
