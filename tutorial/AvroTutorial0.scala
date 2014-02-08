/**
Scalding with Avro (and Json) tutorial part 0.

To run this job:
  scripts/scald.rb --local --avro --json tutorial/AvroTutorial0.scala

Check the output:
  cat tutorial/data/avrooutput0.avro

**/

import com.twitter.scalding.{Job, Args, JsonLine}
import com.twitter.scalding.avro.UnpackedAvroSource
import org.apache.avro.Schema
 
class parseJson(args: Args) extends Job(args) {
  val schema = """{
"type": "record", "name": "parseJson", "fields": [
{ "name": "sessionId", "type": "string" },
{ "name": "optionalField", "type": ["string", "null"] }
] }"""

  JsonLine("tutorial/data/session.json", ('sessionId, 'optionalField)).read
    .write(UnpackedAvroSource("tutorial/data/avrooutput0.avro", new Schema.Parser().parse(schema)))
}
