package com.twitter.scalding.avro

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.avro.AvroSerializer
import com.twitter.chill.config.{ ConfiguredInstantiator, Config }
import com.twitter.scalding.serialization.KryoHadoop
import com.twitter.scalding.Job
import org.apache.avro.specific.SpecificRecordBase

/**
 * @author Mansur Ashraf.
 */
trait AvroJob { self: Job =>
  override def config: Map[AnyRef, AnyRef] = self.config ++ Map(ConfiguredInstantiator.KEY -> classOf[AvroKryo].getName)
}

class AvroKryo(config: Config) extends KryoHadoop(config) {

  override def newKryo: Kryo = {
    val kryo = super.newKryo
    kryo.register(classOf[SpecificRecordBase], AvroSerializer.SpecificRecordSerializer[SpecificRecordBase])
    kryo
  }
}