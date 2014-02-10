/**
Scalding with Avro (and Json) tutorial part 0.

To run this job:
  scripts/scald.rb --local --avro --json tutorial/AvroTutorial0.scala

Check the output:
  java -jar avro-tools-1.7.6.jar tojson tutorial/data/avrooutput0.avro

**/

import com.twitter.scalding.{Job, Args, JsonLine}
import com.twitter.scalding.avro.UnpackedAvroSource
import org.apache.avro.Schema
 
class AvroTutorial0(args: Args) extends Job(args) {
  val schema = """{
"type": "record", "name": "parseJson", "fields": [
{ "name": "sessionId", "type": "string" },
{ "name": "optionalField", "type": ["string", "null"] }
] }"""

  JsonLine("tutorial/data/session.json", ('sessionId, 'optionalField)).read
    .write(UnpackedAvroSource("tutorial/data/avrooutput0.avro", new Schema.Parser().parse(schema)))
}
