/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
import com.twitter.scalding._

/**
Scalding tutorial part 6.

This is similar to Tutorial1 except that we show the use of Scala Enumerations to specify fields.

To run this job:
  scripts/scald.rb --local tutorial/Tutorial6.scala

Check the output:
  cat tutorial/data/output6.tsv

**/

class Tutorial6(args : Args) extends Job(args) {
  /** When a data set has a large number of fields, and we want to specify those fields conveniently
    in code, we can use, for example, a Tuple of Symbols (as most of the other tutorials show), or a List of Symbols.
    Note that Tuples can only be used if the number of fields is at most 22, since Scala Tuples cannot have more
    than 22 elements. Another alternative is to use Enumerations, which we show here **/

  object Schema extends Enumeration {
    val first, last, phone, age, country = Value // arbitrary number of fields
  }

  import Schema._

  object Other extends Enumeration {
    val full = Value
  }

  import Other._

  Csv("tutorial/data/phones.txt", separator = " ", fields = Schema)
    .read
    .map((first, last) -> full) { name: (String, String) => name._1 + ' ' + name._2 }
    .project(full, age)
    .write(Tsv("tutorial/data/output6.tsv"))
}

