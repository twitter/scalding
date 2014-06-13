import cascading.pipe.Pipe
import com.twitter.scalding._

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
  
  args("tutorial") match {
    
    /**
    Tutorial {0,1}: Write out to a TSV file.
    ----------------------------------------
    In this first version we will be as explicit as possible to show all
    the steps required to go from a raw text file to a typed stream.
    **/
    case "0" | "1" => {
      
      // The TextLine source splits the input by lines.
      val textSource = TextLine(args("input"))
      
      // Create a type-safe pipe from the TextLine.
      val lines: TypedPipe[String] = 
        TypedPipe.from[String](textSource) 
      
      // Write the typed pipe out to a tab-delimited file.
      lines.write(TypedTsv[String](args("output")))
    }
    
    /**
    Tutorial 2: Simple map
    ----------------------
    Reverse all the strings. Notice that we've now left off the [String] type.
    Scala can generally infer these types for us, making the code cleaner.
    **/
    case "2" | "map" => {
      // Create a typed pipe from the TextLine (of type TypedPipe[String] still)
      TypedPipe.from(TextLine(args("input")))
        // Transform each line, reversing it. Output is a new TypedPipe, still of String.
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
      TypedPipe.from(TextLine(args("input")))
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
      val words = TypedPipe.from(TextLine(args("input")))
                           .flatMap(_.split("\\s"))
      
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
      val counts = groups.size
      
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
        // For TypedTsv, Scalding coerces the fields to the specified types,
        // throwing an exception if any line fails.
        TypedPipe.from(TypedTsv[(String,Double)](args("words")))
          // group by word so we can join it
          .group
      
      // get the lines, this time from an 'OffsetTextLine' which is a
      // typed wrapper on 'TextLine' that contains the 'byte offset' and
      // text of each line in the file.
      val lines: TypedPipe[(Long,String)] = TypedPipe.from(OffsetTextLine(args("input")))
      
      // Split lines into words, but keep their original line offset with them.
      val wordsWithLine : Grouped[String,Long] =
        lines
          .flatMap{ case (offset, line) =>
            // split into words
            line.split("\\s")
            // keep the line offset with them
              .map(word => (word.toLowerCase, offset))
          }
          // make the 'word' field the key
          .group
      
      // Associate scores with each word; merges the two value types into
      // a tuple: [String,Long] join [String,Double] -> [String,(Long,Double)]
      val scoredWords = wordsWithLine.join(scores)
      
      // get scores for each line (indexed by line number)
      val scoredLinesByNumber = 
        scoredWords
          // select the line offset and score fields
          .map{ case (word,(offset,score)) => (offset,score) }
          // group by line offset (groups all the words for a line together)
          .group
          // compute total score per line
          .sum
          // Group and sum are often run together in this way.
          // The `sumByKey` operation performs performs both.
      
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
    Interoperability with Fields API
    --------------------------------
    Scalding also provides a thinner, un-type-safe wrapper over Cascading 
    which is known as the Fields API because each record has a number of 
    named "fields".
    
    Most jobs can be done completely in the Typed API, but for compatibility,
    there are ways to go back and forth between the two schemes, which the
    next couple cases demonstrate.
    **/
    
    /**
    Pipe vs. TypedPipe
    ------------------
    TypedPipes can be easily converted to Pipes and vice-versa.
    **/
    case "pipes" => {
      // calling 'read' on a source returns an un-typed Pipe
      // TextLine, by default, contains two fields: 'offset, and 'line.
      val rawPipe: Pipe = TextLine(args("input")).read
      
      // To convert to a typed pipe, we must specify the fields we want 
      // and their types:
      val lines: TypedPipe[(Long,String)] =
        TypedPipe.from[(Long,String)](rawPipe, ('offset,'line))
      
      // We can operate on this typed pipe as above, and come up with a 
      // different set of fields
      val lineSizes: TypedPipe[Long] = lines.map{ case (offset,line) => line.length }
      
      // To convert back to a Fields Pipe, we must specify the names of the fields:
      val lineSizesField: Pipe = lineSizes.toPipe('size)
      
      // finally, we can write out this untyped pipe with an untyped sink:
      lineSizesField.write(Tsv(args("output")))
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
      // Get the .typed enrichment
      import TDsl._
      
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
