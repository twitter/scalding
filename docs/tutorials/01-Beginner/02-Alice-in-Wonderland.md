# Alice in Wonderland Tutorial

First, let's import some stuff.

```tut:silent
import scala.io.Source
import com.twitter.scalding._
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.ReplImplicitContext._
```

```tut
val alice = Source.fromURL("https://raw.githubusercontent.com/mihi-tr/reading-alice/master/pg28885.txt").getLines
```

Add the line numbers, which we might want later
```tut
val aliceLineNum = alice.zipWithIndex.toList
```

Now for scalding, TypedPipe is the main scalding object representing
your data.

```tut
val alicePipe = TypedPipe.from(aliceLineNum)

val aliceWordList = alicePipe.map { line => line._1.split("\\s+") }
```

Three things: map, function, tuples
but that's ugly, so we can use tuple matching the be clearer:

```tut
val aliceWordList = alicePipe.map { case (text, lineNum) =>
  text.split("\\s+").toList
}
```

But we want words, not lists of words. We need to flatten!
```tut
val aliceWords = aliceWordList.flatten
```

Scala has a common function for this map + flatten == flatMap
```tut
val aliceWords = alicePipe.flatMap { case (text, _) => text.split("\\s+").toList }
```

Now lets add a count for each word:
```tut
val aliceWithCount = aliceWords.map { word => (word, 1L) }
```
let's sum them for each word:
```tut
val wordCount = aliceWithCount.group.sum
```

(We could have also used `.sumByKey`, which is equivalent to `.group.sum`.)

Let's print them to the screen (REPL only):
```tut
wordCount.toIterator.take(100)
```

Let's print just the ones with more that 100 appearances:
```tut
wordCount.filter { case (word, count) => count > 100 }.dump
```

But which is the biggest word?

> Hint: In the Scala REPL, you can turn on `:paste` mode to make it easier to paste multi-line expressions.

```tut
val top10 = { wordCount
      .groupAll
      .sortBy { case (word, count) => -count }
      .take(10) }

top10.dump
```

Where is Alice? What is with the ()?

```tut
val top20 = { wordCount
      .groupAll
      .sortBy { case (word, count) => -count }
      .take(20)
      .values } // ignore the ()-all key

top20.dump
```

There she is!

Now, suppose we want to know the last line on which each word appears.

How do we solve this?
First, we generate `(word, lineNum)` pairs by  flatmapping each line of words to a list of `(word, lineNum)` pairs.

```tut
val wordLine = alicePipe.flatMap { case (text, line) =>
   text.split("\\s+").toList.map { word => (word, line) }
 }
```

Next, we group the pairs on the word, and take the max line number for each group.

> See all the functions on grouped things here:
> [http://twitter.github.io/scalding/#com.twitter.scalding.typed.Grouped](http://twitter.github.io/scalding/#com.twitter.scalding.typed.Grouped)

```tut
val lastLine = wordLine.group.max
```

Finally, we lookup the words from the initial line:

> By the way: `lastLine.swap` is equivalent to `lastLine.map { case (word, lastLine) => (lastLine, word) }`

```tut
val words = {
  lastLine.map { case (word, lastLine) => (lastLine, word) }
          .group
          .join(alicePipe.swap.group)
}

println(words.toIterator.take(30).mkString("\n"))
```

That's it.
You have learned the basics:
TypedPipe, map/flatMap/filter
groups do reduce/join: max, sum, join, take, sortBy
