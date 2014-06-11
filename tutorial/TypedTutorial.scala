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
		  --words tutorial/data/words.txt \
			--output tutorial/data/output0.txt
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
			
		  val input_raw = TextLine(args("input"))
		  val out_file_typed = args("output").replace(".txt",".typed.txt")
  
		  // `TextLine.read` creates an (untyped) Pipe with one field named 'line.
		  val raw_pipe: cascading.pipe.Pipe = input_raw.read
  
		  // Convert that into a type-safe pipe (each line is a String)
		  val typed_pipe: TypedPipe[String] = raw_pipe.toTypedPipe[String]('line) 
			
      typed_pipe.write(TypedTsv[String](args("output")))
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
				.map{ _.reverse }
				// The types for the TypedTsv can be inferred, so we don't even
				// need to specify them here. However, it's good to specify them
				// anyway so that if the output type changes, it is detected.
        .write(TypedTsv(args("output")))
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
        .flatMap{ _.split("\\s") }
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
      val words = TextLine(args("input")).flatMap{ _.split("\\s") }
      
      // To count the words, we use TypedPipe's `groupBy` method.
      // However, this no longer returns a `TypedPipe[T]`, but rather
      // a `Grouped[K,T]` based on the type of the key used to group by.
      // 
      // groupBy accepts a function to determine the key for grouping. 
      // In the case of word count, let's imagine we want to make sure 
      // capitalization doesn't matter, so to come up with the key, 
      // we normalize it to lower case.
      val groups : Grouped[String,String] = words.groupBy{ _.toLowerCase }
      
      // Next we specify what to do with each aggregation. In the case
      // of word count, we simply want the size of each group. This
      // operation results in a new `Grouped` that has the key (String, 
      // the lower case words), and the counts (Long).
      //
      // Note: if we wanted to do a more interesting aggregation, in the
      // type-safe API we'd define a Monoid for our object and call `sum`.
      // See the wiki for more details: https://github.com/twitter/scalding/wiki/Type-safe-api-reference#aggregation-and-stream-processing
      val counts : typed.UnsortedGrouped[String,Long] = groups.size
      
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
      // Load the word scores; TextLine produces a line number in the 
			// "offset" field, which we will keep around this time and use
			// as the "score" for the word.
      val scores: Grouped[String,Double] =
        TextLine(args("words")).read
          .toTypedPipe[(Long,String)]('offset,'line)
          
          // re-pack the scores "indexed" on the lower-cased word
          .map{ case (score, word) => (word.toLowerCase, score.toDouble) }
          .group // treat the first field as the key, second as value
      
      val lines = TextLine(args("input"))
										// explicitly keep around 'offset so we can group by it
										.read.toTypedPipe[(Long,String)]('offset,'line)
      
      val words_by_line : Grouped[String,Long] =
        lines
          .flatMap{ case (offset, line) =>
            // split into words, keeping the line number ("offset") with them
            line.split("\\s").map{ word => (word.toLowerCase, offset) }
          }
          // make the 'word' field the key
          .group
			
      // Associate scores with each word.
			val scored_words : typed.CoGrouped[String,(Long,Double)] =
				words_by_line.join(scores)
      
			// get scores for each line (indexed by line number)
			val scored_lines_by_number = 
				scored_words
					// "project" only offset (line number) and score fields
					.map{ case (word,(offset,score)) => (offset,score) }
					// group by offset (line number)
					.group
					// compute total score per line
					.sum
			
			// Associate the original line text with the computed score,
			// discard the 'offset' field
			val scored_lines: TypedPipe[(String,Double)] =
				lines
					// index lines by 'offset'
					.group
					// associate scores with offsets
					.join(scored_lines_by_number)
					// take just the value fields (discard the 'offset' (line number))
					.values
			
			// write out the final result
      scored_lines.write(TypedTsv[(String,Double)](args("output")))
			
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
	        tp.map{ _.length }
	      }
				// the final output will have just the 'size field
				// and can be dumped using the un-typed Tsv source.
				.write(Tsv(args("output")))
    }
  }
}
