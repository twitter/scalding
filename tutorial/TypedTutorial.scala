import cascading.pipe.Pipe
import com.twitter.scalding._
import com.twitter.scalding.typed.{UnsortedGrouped,CoGrouped}

/**
Scalding Tutorial ported to use the Type-safe API (TDsl)
(rather than Cascading's Fields API). The examples here roughly correspond
to those in `tutorial/Tutorial{0..5}.scala`.

These tutorials are all run from this single file; which one is run can
be chosen with a command-line flag "--tutorial". For instance, to run the
first tutorial example:

> ./scripts/scald.rb --local tutorial/TypedTutorial.scala \
      --tutorial 0 \
      --input tutorial/data/hello.txt \
      --output tutorial/data/output0.txt \
      --words tutorial/data/word_scores.tsv

(Note: only tutorial 5 uses "word_scores.tsv")
**/
class TypedTutorial(args : Args) extends Job(args) {
  // Import the DSL implicits inside a scope so they don't leak out and
  // cause mysterious behaviour elsewhere.
  import TDsl._
  
  args("tutorial") match {
    
    /**
    Tutorial {0,1}: Write out to a TSV file.
    ----------------------------------------
    In this first version we will be as explicit as possible to show all
    the steps required to go from a raw text file to a typed stream.
    
    Note: `toTypedPipe` is effectively the type-safe version of `project`, 
    so "Tutorial 0" and "Tutorial 1" are the same for the Type-safe API.
    **/
    case "0" | "1" => {
      
      val rawText = TextLine(args("input"))
  
      // `TextLine.read` creates an (untyped) Pipe with two fields:
      // 'offset (the line number), and 'line (the text of the line)
      val rawPipe: Pipe = rawText.read
      
      // Create a type-safe pipe from the raw pipe, selecting just the
      // 'line field (which is a String)
      val lines: TypedPipe[String] = 
        TypedPipe.from[String](rawPipe, 'line) 
      
      // Write the typed pipe out to a tab-delimited file.
      lines.write(TypedTsv[String](args("output")))
    }
    
    /**
    Tutorial 2: Simple map
    ----------------------
    Reverse all the strings. Here we also rely on implicit conversions 
    baked into the Typed DSL to make it less verbose
    **/
    case "2" | "map" => {
      TextLine(args("input"))
        // TextLine is implicitly read and converted to TypedPipe[String]
        // (taking the 'line field) to pass to `map` here
        .map(_.reverse)
        // Note, the types for the TypedTsv *can* be inferred by Scala here.
        // However, it's best to specify them explicitly so that if the
        // output type changes, it is detected and doesn't break the next
        // thing to read from the output file.
        .write(TypedTsv[String](args("output")))
    }
    
    /**
    Tutorial 3: Flat Map
    ---------------------
    Dump all the words.
    **/
    case "3" | "flatmap" => {
      TextLine(args("input"))
        // flatMap is like map, but instead of returning a single item
        // from the function, we return a collection of items. Each of
        // these items will create a new entry in the data stream; here,
        // we'll end up with a new entry for each word.
        .flatMap(_.split("\\s"))
        // output of flatMap is still a collection of String
        .write(TypedTsv[String](args("output")))
    }
    
    /**
    Tutorial 4: Word Count
    ----------------------
    Now that we have a stream of words, clearly we're ready for
    that most exciting of MapReduce examples: the Word Count.
    **/
    case "4" | "wordcount" => {
      // Get the words (just like above in case "3")
      val words = TextLine(args("input")).flatMap(_.split("\\s"))
      
      // To count the words, we use TypedPipe's `groupBy` method.
      // However, this no longer returns a `TypedPipe[T]`, but rather
      // a `Grouped[K,T]` based on the type of the key used to group by.
      // 
      // groupBy accepts a function to determine the key for grouping. 
      // In the case of word count, let's imagine we want to make sure 
      // capitalization doesn't matter, so to come up with the key, 
      // we normalize it to lower case.
      val groups : Grouped[String,String] = words.groupBy(_.toLowerCase)
      
      // Next we specify what to do with each aggregation. In the case
      // of word count, we simply want the size of each group. This
      // operation results in a new `Grouped` that has the key (String, 
      // the lower case words), and the counts (Long).
      //
      // Note: To do more interesting aggregations, Scalding supports
      // a variety of operations, such as `sum`, `reduce`, `foldLeft`,
      // `mapGroup`, etc, that can all be applied efficiently on Monoids
      // (primitives like Long, container types like `Map`, or custom
      // monoids you define yourself). See the wiki for more details:
      // https://github.com/twitter/scalding/wiki/Type-safe-api-reference
      val counts : UnsortedGrouped[String,Long] = groups.size
      
      // And finally, we dump these results to a TypedTsv with the 
      // correct Tuple type.
      counts.write(TypedTsv[(String,Long)](args("output")))
    }
    
    /**
    Tutorial 5: Demonstrate joins
    -----------------------------
    Associate a score with each word and compute a score for each line.
    
    Note: this example is a bit contrived, but serves to demonstrate
    how to combine multiple input sources.
    **/
    case "5" | "join" => {
      // Load the scores for each word from TSV file and group by word.
      val scores: Grouped[String,Double] =
        // Scalding coerces the fields it finds to the specified types,
        // throwing an exception if anything fails.
        TypedTsv[(String,Double)](args("words"))
          // group by word so we can join it
          .group
      
      // get the lines
      val lines: TypedPipe[(Long,String)] = 
        // (workaround until TextLine is changed to be Mappable[(Long,String)])
        TypedPipe.from[(Long,String)](TextLine(args("input")).read,('offset,'line))
        
      val wordsByLine : Grouped[String,Long] =
        lines
          .flatMap{ case (offset, line) =>
            // split into words
            line.split("\\s")
            // keep the line offset with them
              .map(word => (word.toLowerCase, offset))
          }
          // make the 'word' field the key
          .group
      
      // Associate scores with each word.
      val scoredWords : CoGrouped[String,(Long,Double)] =
        wordsByLine.join(scores)
      
      // get scores for each line (indexed by line number)
      val scoredLinesByNumber = 
        scoredWords
          // select the line offset and score fields
          .map{ case (word,(offset,score)) => (offset,score) }
          // group by line offset (groups all the words for a line together)
          .group
          // compute total score per line
          .sum
      
      // Associate the original line text with the computed score,
      // discard the 'offset' field
      val scoredLines: TypedPipe[(String,Double)] =
        lines
          // index lines by 'offset'
          .group
          // associate scores with lines (by offset)
          .join(scoredLinesByNumber)
          // take just the value fields (discard the 'line offset')
          .values
      
      // write out the final result
      scoredLines.write(TypedTsv[(String,Double)](args("output")))
      
    }
        
    /**
    Bonus: Typed blocks
    -------------------
    An alternative to working completely in typed mode is to use
    `typed` blocks, which create a TypedPipe within the scope, and then
    map the output back into an untyped Pipe. You specify the fields to 
    map in and out using the `->` pair passed to `typed()`.
    **/
    case "block" => {
      TextLine(args("input")).read
        .typed('line -> 'size) { tp: TypedPipe[String] =>
          // now operate on the typed pipe
          tp.map(_.length)
        }
        // the final output will have just the 'size field
        // and can be dumped using the un-typed Tsv source.
        .write(Tsv(args("output")))
    }
  }
}
