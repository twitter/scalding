# Common Exceptions and Possible Reasons

### java.lang.NoClassDefFoundError
This is probably when you are missing a dependency, or have a problem with the binary version of a dependency. Some libraries required at run time, such as Hadoop and many of Hadoop's includes, are expected to be on the classpath and are not bundled by Scalding itself. So check for that.

### java.lang.AbstractMethodError
This probably is also a jar version problem.

### java.lang.NoSuchMethodError
This probably is also a jar version problem.

### java.lang.ClassNotFoundException
1. Your classpath or jar is missing a needed class. Check your build, or perhaps the name of the job you are trying to run.
1. Make sure you specified the fully packaged name of the class you wish to run package.JobName and not just JobName.

### java.lang.UnsupportedClassVersionError
You have compiled your code with a later version of javac/scalac than is supported by the java vm you are attempting to run with. You need to either downgrade the compiler or upgrade the java installation.

### java.util.zip.ZipException
   : invalid CEN header (bad signature)

This is the jar file exceeding 64k problem. The solution is to use --bundle option of scald.rb.

### cascading.flow.planner.PlannerException
Cascading requires all sources to have final sinks on disk.
This exception happens when you miss an output for an input.

It also could signify an attempt to write an unserializable datatype.

### com.twitter.scalding.InvalidSourceException
This happens when data is missing from the path you provied.

### java.lang.RuntimeException: Please only provide a single value for --some input key here
Try putting quotes around your input value

### cascading.flow.FlowException: step failed: (1/2) .../algos/4/data/userIds.tsv, with job id: ...
Common exception when running hadoop commands with sudo.
Check that you have given permissions for the user (root) in hdfs
`sudo addgroup supergroup; sudo adduser root supergroup`
