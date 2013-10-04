This module contains Avro support for Scalding. It's based on the cascading.avro project,
https://github.com/ScaleUnlimited/cascading.avro .

In some case Kryo (the default serializer used by Scalding) doesn't work well with Avro objects. If you run in to
serialization errors, or if you want to preempt and trouble, you should add the following to your Job class:
```scala
override def ioSerializations = super.ioSerializations :+ "cascading.avro.serialization.AvroSpecificRecordSerialization"
```

This will use cascading.avro's Avro SpecificRecord serialization for Avro objects in place of the Kryo serialization.

