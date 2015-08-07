# Using Scalding with other versions of Scala

There are two places you need to change in order to make this run correctly:

1. Change the scala version in the build.sbt in the project root.
2. Since scald.rb calls the compiler on your script, it needs to match the version you use above.  Edit the top of scald.rb to change SCALAC to point to the version of the compiler that matches the project (sorry about this, patches accepted).
