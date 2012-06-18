# Cascalog

[![Build Status](https://secure.travis-ci.org/nathanmarz/cascalog.png?branch=develop)](http://travis-ci.org/nathanmarz/cascalog)

Cascalog is a fully-featured data processing and querying library for Clojure or Java. The main use cases for Cascalog are processing "Big Data" on top of Hadoop or doing analysis on your local computer. Cascalog is a replacement for tools like Pig, Hive, and Cascading and operates at a significantly higher level of abstraction than those tools.

Follow the getting started steps, check out the tutorial, and you'll be running Cascalog queries on your local computer within 5 minutes.

# Getting Started with JCascalog

To get started with JCascalog, Cascalog's pure-Java API, see [this wiki page](https://github.com/nathanmarz/cascalog/wiki/JCascalog). The jcascalog.Playground class has in-memory datasets that you can play with to learn the basics.

# Getting started with Clojure Cascalog

The best way to get started with Cascalog is experiment with the toy datasets that ship with the project. These datasets are served from memory and can be played with purely from the REPL. Just follow these steps and you'll be on your way:

1. Install [leiningen](http://github.com/technomancy/leiningen)
2. Make sure you have Java 1.6 (run `java -version`)
3. checkout the Cascalog project using Git
4. `lein deps, compile, repl`
5. Work through the examples in the [introductory](http://nathanmarz.com/blog/introducing-cascalog-a-clojure-based-query-language-for-hado.html) [tutorials](http://nathanmarz.com/blog/new-cascalog-features-outer-joins-combiners-sorting-and-more.html)

# Using Cascalog within a project

Cascalog is hosted at [Clojars](http://clojars.org/cascalog). Clojars is a maven repo that's easy to use with maven or leiningen.

To include Cascalog in your leiningen or cake project, add the following to your `project.clj`:

    [cascalog "1.9.0"] ;; under :dependencies
    [org.apache.hadoop/hadoop-core "0.20.2-dev"] ;; under :dev-dependencies

Note that Cascalog is compatible with Clojure 1.2.0, 1.2.1, 1.3.0, and 1.4.0. 
    
# Documentation and Issue Tracker

- The [Cascalog Wiki](https://github.com/nathanmarz/cascalog/wiki) contains more information and links to Various articles and tutorials.
- API documentation can be found at http://nathanmarz.github.com/cascalog/.
- [Issue Tracker on Github](https://github.com/nathanmarz/cascalog/issues).

Come chat with us in the Google group: [cascalog-user](http://groups.google.com/group/cascalog-user)

Or in the #cascalog or #cascading rooms on freenode!

# Priorities for Cascalog development

1. Replicated and bloom joins
2. Cross query optimization: push constants and filters down into subqueries when possible

# Acknowledgements

YourKit is kindly supporting open source projects with its full-featured Java Profiler. YourKit, LLC is the creator of innovative and intelligent tools for profiling Java and .NET applications. Take a look at YourKit's leading software products: [YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and [YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).

Cascalog is based off of a very early branch of cascading-clojure project (http://github.com/clj-sys/cascading-clojure). Special thanks to Bradford Cross and Mark McGranaghan for their work on that project. Much of that code appears within Cascalog in either its original form or a modified form.
