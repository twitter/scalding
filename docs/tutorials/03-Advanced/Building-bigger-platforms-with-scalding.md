# Building Bigger Platforms with Scalding

As of scalding 0.12, we have an API for this around the Execution type. It is described
in [[Calling-Scalding-from-inside-your-application]]
This is the recommended approach because it is type-safe, and allows you to compose multiple Executions together.

## Using the Fields-API outside of a Job constructor
We consider the Fields-API to be a legacy API which is in maintenance mode. If you really need to use it in new code
be aware that sharing code between jobs in the Fields API is
a bit challenging because you have to be careful about what fields you leave in the Pipe and there is little help from the compiler.

Generally you will write functions that take `Pipe`s and `Fields` and return `Pipe`s and `Field`s. Any time you are reading or writing data, you will need to take `(implicit flow: FlowDef, mode: Mode)` as implicit arguments to your methods. To get the Dsl syntax, you will want to `import com.twitter.scalding.Dsl._` in any file or object that has this shared code.

## Customizing Job execution

Mention specialized Job examples (CascadeJob for instance).

## Using scalding outside of a com.twitter.scalding.Job (or Tool)

Just do what you would with cascading:
```scala
        implicit val mode = Hdfs(new JobConf())
        implicit val flowDef = new FlowDef
        flowDef.setName(jobName)
        val result = myFunctionThatTakesFlowDefAndMode(flowDef, mode)
        // Now we have a populated flowDef, time to let Cascading do it's thing:
        mode.newFlowConnector(config).connect(flowDef).complete
```

## Required settings outside of a com.twitter.scalding.Job (or Tool)

com.twitter.scalding.Job makes changes to the config passed into the Cascading FlowConnector for scalding to function properly. When using scalding outside of a com.twitter.scalding.Job you need to set these.
```scala
val config: Map[AnyRef, AnyRef] = Map(
  "io.serializations" -> "org.apache.hadoop.io.serializer.WritableSerialization,cascading.tuple.hadoop.TupleSerialization,com.twitter.chill.hadoop.KryoSerialization",
  "com.twitter.chill.config.configuredinstantiator" -> "com.twitter.scalding.serialization.KryoHadoop",
  "cascading.flow.tuple.element.comparator" -> "com.twitter.scalding.IntegralComparator")
mode.newFlowConnector(config).connect(flowDef).complete
```
