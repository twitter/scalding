# Scalding
Scalding is a library that has two components:

* a scala DSL to make map-reduce computations look very similar to scala's collection API
* a wrapper to Cascading to make simpler to define the usual use cases of jobs, tests and describing new data on HDFS.

To run scala scalding jobs, a script, scald.rb is provided in scripts/. Run this script
with no arguments to see usage tips.  You will need to customize the default variables
at the head of that script for your environment.

You should follow the scalding project on twitter: <http://twitter.com/scalding>

## Word Count
Hadoop is a distributed system for counting words.  Here is how it's done in scalding.  You can find this in examples:

    package com.twitter.scalding.examples
    
    import com.twitter.scalding._
    
    class WordCountJob(args : Args) extends Job(args) {
      TextLine( args("input") ).read.
        flatMap('line -> 'word) { line : String => line.split("\\s+") }.
        groupBy('word) { _.size }.
        write( Tsv( args("output") ) )
    }

##Tutorial
See tutorial/ for examples of how to use the DSL.  See tutorial/CodeSnippets.md for some
example scalding snippets.

## Building
0. Install sbt 0.7.4
1. sbt update (takes 2 minutes or more)
2. sbt test
3. sbt package-dist

use "sbt assembly" if you need to make a fat jar with all dependencies (recommended to work with
scald.rb in scripts).

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