# REPL in Wonderland
====================

To build and launch the repl:

```
./sbt scalding-repl/assembly
./scripts/scald.rb --repl --local
```

Now that your repl is up, let's get started!

```scala
import scala.io.Source
val alice = Source.fromURL("http://www.gutenberg.org/files/11/11.txt").getLines
// Add the line numbers, which we might want later
val aliceLineNum = alice.zipWithIndex.toList
// Now for scalding, TypedPipe is the main scalding object representing
// your data.
val alicePipe = TypedPipe.from(aliceLineNum)

val aliceWordList = alicePipe.map { line => line._1.split("\\s+").toList }
// Three things: map, function, tuples

// but that's ugly, so we can use tuple matching the be clearer:
val aliceWordList = alicePipe.map { case (text, lineno) =>
  text.split("\\s+").toList
}

// But we want words, not lists of words. We need to flatten!
val aliceWords = aliceWordList.flatten

// Scala has a common function for this map + flatten == flatMap
val aliceWords = alicePipe.flatMap { case (text, _) => text.split("\\s+").toList }

// Now lets add a count for each word:
val aliceWithCount = aliceWords.map { word => (word, 1L) }
// let's sum them for each word:
val wordCount = aliceWithCount.group.sum
// or: .group.sum == .sumByKey

// let's print them to the screen (REPL only)
wordCount.dump

// Let's print just the ones with more that 100 appearances:
wordCount.filter { case (word, count) => count > 100 }.dump

// but which is the biggest word?
// use, :paste to put multi-line expressions
val top10 = wordCount
  .groupAll
  .sortBy { case (word, count) => -count }
  .take(10)

top10.dump

// Where is Alice? What is with the ()?
// use, :paste to put multi-line expressions
val top20 = wordCount
      .groupAll
      .sortBy { case (word, count) => -count }
      .take(20)
      .values // ignore the ()-all key

top20.dump
// there she is!

// what is the last line, on which each word appears?

/**
 * How to solve this?
 * (flat)map text to (word, lineno) pairs
 * for each word, take the maximum line num
 * then join the line number to the original input
 */
val wordLine = alicePipe.flatMap { case (text, line) =>
   text.split("\\s+").toList.map { word => (word, line) }
 }
 // Take the max
 // see all the functions on grouped things here:
 // http://twitter.github.io/scalding/#com.twitter.scalding.typed.Grouped

val lastLine = wordLine.group.max

// now lookup the initial line:
lastLine.map { case (word, lastLine) => (lastLine, word) }
 // same as .swap, by the way
  .group
  .join(alicePipe.swap.group)
  .dump
```

That's it. You have learned the basics:
* TypedPipe, map/flatMap/filter
* Groups do reduce/join: max, sum, join, take, sortBy
