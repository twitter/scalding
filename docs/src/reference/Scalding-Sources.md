# Scalding Sources

Scalding sources are how you get data into and out of your scalding jobs. There are several useful sources baked into the project and a few more in the [scalding-commons](https://github.com/twitter/scalding/tree/develop/scalding-commons/src/main/scala/com/twitter/scalding/commons/source) repository. Here are a few basic ones to get you started:

* To read a text file line-by-line, use `TextLine(filename)`.  For every line in `filename`, this source creates a tuple with two fields:
    * `line` contains the text in the given line
    * `offset` contains the byte offset of the given line within `filename`
* To read or write a tab- or comma-separated values file, use `Tsv` or `Csv`.
    * When reading a `Tsv` or `Csv`, Scalding will choose field names based on the input file's headers.
    * When writing a `Tsv` or `Csv`, Scalding will write out headers with the field names.
* To create a pipe from data in a Scala `Iterable`, use the `IterableSource`.  For example, `IterableSource(List(4,8,15,16,23,42), 'foo)` will create a pipe with a field `'foo`.  `IterableSource` is especially useful for unit testing.
* A `NullSource` is useful if you wish to create a pipe for only its side effects (e.g., printing out some debugging information).  For example, although defining a pipe as `Csv("foo.csv").debug` without a sink will create a `java.util.NoSuchElementException`, adding a write to a `NullSource` will work fine: `Csv("foo.csv").debug.write(NullSource)`.
