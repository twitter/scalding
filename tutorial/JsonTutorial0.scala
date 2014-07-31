/**
Scalding with Json tutorial part 0.

To run this job:
  scripts/scald.rb --local --json tutorial/JsonTutorial0.scala

Check the output:
  cat tutorial/data/jsonoutput0.tsv

**/

import com.twitter.scalding.{Job, Args, JsonLine, Tsv}
 
class JsonTutorial0(args: Args) extends Job(args) {
  JsonLine("tutorial/data/session.json", ('sessionId)).read
    .groupBy('sessionId){_.size}
    .write(Tsv("tutorial/data/jsonoutput0.tsv"))
}
