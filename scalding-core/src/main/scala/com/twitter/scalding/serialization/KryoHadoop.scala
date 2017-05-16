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
package com.twitter.scalding.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.FieldSerializer
import com.twitter.scalding.{ Args, CascadingTokenUpdater, DateRange, RichDate, Config => ScaldingConfig }
import com.twitter.chill.algebird._
import com.twitter.chill.config.Config
import com.twitter.chill.{ IKryoRegistrar, KryoInstantiator, ScalaKryoInstantiator, SingletonSerializer }

class KryoHadoop(@transient config: Config) extends KryoInstantiator {
  // keeping track of references is costly for memory, and often triggers OOM on Hadoop
  val useRefs = config.getBoolean("scalding.kryo.setreferences", false)
  val cascadingSerializationTokens = config.get(ScaldingConfig.CascadingSerializationTokens)

  /**
   * TODO!!!
   * Deal with this issue.  The problem is grouping by Kryo serialized
   * objects silently breaks the results.  If Kryo gets in front of TupleSerialization
   * (and possibly Writable, unclear at this time), grouping is broken.
   * There are two issues here:
   * 1) Kryo objects not being compared properly.
   * 2) Kryo being used instead of cascading.
   *
   * We must identify each and fix these bugs.
   */
  override def newKryo: Kryo = {
    val newK = (new ScalaKryoInstantiator).newKryo
    // These are scalding objects:
    newK.register(classOf[RichDate], new RichDateSerializer())
    newK.register(classOf[DateRange], new DateRangeSerializer())
    newK.register(classOf[Args], new ArgsSerializer)
    // Some of the monoids from Algebird that we use:
    newK.register(classOf[com.twitter.algebird.AveragedValue], new AveragedValueSerializer)
    newK.register(classOf[com.twitter.algebird.DecayedValue], new DecayedValueSerializer)
    newK.register(classOf[com.twitter.algebird.HyperLogLogMonoid], new HLLMonoidSerializer)
    newK.register(classOf[com.twitter.algebird.Moments], new MomentsSerializer)
    newK.addDefaultSerializer(classOf[com.twitter.algebird.HLL], new HLLSerializer)
    // Don't serialize Boxed instances using Kryo.
    newK.addDefaultSerializer(classOf[com.twitter.scalding.serialization.Boxed[_]], new ThrowingSerializer)

    // Register every boxed class so they are given cascading tokens
    for {
      boxedClass <- Boxed.allClasses
    } {
      newK.register(boxedClass, new ThrowingSerializer)
    }

    /**
     * AdaptiveVector is IndexedSeq, which picks up the chill IndexedSeq serializer
     * (which is its own bug), force using the fields serializer here
     */
    newK.register(classOf[com.twitter.algebird.DenseVector[_]],
      new FieldSerializer[com.twitter.algebird.DenseVector[_]](newK,
        classOf[com.twitter.algebird.DenseVector[_]]))

    newK.register(classOf[com.twitter.algebird.SparseVector[_]],
      new FieldSerializer[com.twitter.algebird.SparseVector[_]](newK,
        classOf[com.twitter.algebird.SparseVector[_]]))

    newK.addDefaultSerializer(classOf[com.twitter.algebird.AdaptiveVector[_]],
      classOf[FieldSerializer[_]])

    /**
     * Pipes can be swept up into closures inside of case classes.  This can generally
     * be safely ignored.  If the case class has a method that actually accesses something
     * in the pipe (what would that even be?), you will get a null pointer exception,
     * so it shouldn't cause data corruption.
     * a more robust solution is to use Spark's closure cleaner approach on every object that
     * is serialized, but that's very expensive.
     */
    newK.addDefaultSerializer(classOf[cascading.pipe.Pipe], new SingletonSerializer(null))

    newK.setReferences(useRefs)

    /**
     * Make sure we use the thread's context class loader to ensure the classes of the
     * submitted jar and any -libjars arguments can be found
     */
    val classLoader = Thread.currentThread.getContextClassLoader
    newK.setClassLoader(classLoader)

    customRegistrar(newK)

    /**
     * Register any cascading tokenized classes not already registered
     */
    val tokenizedClasses =
      CascadingTokenUpdater
        .parseTokens(cascadingSerializationTokens)
        .toList
        .sorted // Go through this list in order the tokens were allocated

    for {
      (id, className) <- tokenizedClasses
      clazz <- getClassOpt(className)
      if !newK.alreadyRegistered(clazz)
    } {
      newK.register(clazz)
    }

    newK
  }

  private def getClassOpt(name: String): Option[Class[_]] =
    try {
      Some(Class.forName(name))
    } catch {
      case _: ClassNotFoundException => None
    }

  /**
   * If you override KryoHadoop, prefer to add registrations here instead of overriding [[newKryo]].
   * That way, any additional default serializers will be used for registering cascading tokenized classes.
   */
  def customRegistrar: IKryoRegistrar = new IKryoRegistrar {
    override def apply(k: Kryo): Unit = {}
  }
}
