# REPL Reference

## Enrichments available on TypedPipe/Grouped/CoGrouped objects:
- `.save(dest: TypedSink[T]): TypedPipe[T]`: runs just the flow to produce this pipe and saves to the given sink, returns a new pipe reading from the source
- `.snapshot: TypedPipe[T]`: Runs just the part of the flow needed to produce the output for this pipe, saves the results to a SequenceFile (if in Hadoop mode), or to memory (in Local mode), and returns a new TypedPipe which reads from the new source.
- `.toIterator`: Returns an iterator. If there is any work to be done to produce the values, it will automatically call `snapshot` first and return an iterator on the snapshot instead.
- `.dump`: Print the contents to stdout (uses `.toIterator`)
- `.toList`: Load the contents into an in-memory List (uses `.toIterator`)

Additionally, `ValuePipe` gets the enrichment `.toOption` which is like `toIterator` but for a single value.

## Repl globals
Everything in a REPL session shares a common FlowDef to allow users to build up a large complicated flow step-by-step and then run it in its entirety. When building a job this way, use `write` (rather than `save`) to add sinks to the global flow.

Importing `ReplImplicits._` makes the following commands available:

- `run`: Runs the overall flow. Trims anything not feeding into a persistent sink (created by `write`). Note: any sinks created but not connected up will cause this to fail. Extra sources, however, are trimmed.
- `resetFlowDef`: Clear the global flowDef. Will require that all pipes/sources/sinks be re-created.

ReplImplicits also includes the execution context, which can be tweaked manually:

- `mode`: Local or Hdfs mode (determined initially by how the repl was launched but can be changed later)
- `config`: Various configuration parameters, including Hadoop configuration parameters specified on the command line, by the Hadoop cluster, or by Scalding.

## Customizing the REPL

- On startup, the REPL searches for files named `.scalding_repl` from the current directory up to the filesystem root and automatically loads them into the current session in order of specificity. Common definitions, values, etc can be put in these files. For instance, one can specify global defaults in their home directory (`~/.scalding_repl`), and project-specific settings in the project directory (`~/workspace/scalding/.scalding_repl`).

- `ScaldingShell.prompt: () => String`: A function to produce the prompt displayed by Scalding. This can also be customized. For instance, to print the current directory at the prompt:

    ```scala
    ScaldingShell.prompt = { () =>
      Console.GREEN +
      System.getProperty("user.dir").replace(System.getProperty("user.home"),"~") +
      Console.BLUE + " scalding> " + Console.RESET
    }
    ```
