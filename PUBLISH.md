# How to push to Sonatype

## Before you push

Follow the instructions [here](http://www.scala-sbt.org/using_sonatype.html). Specifically:
* You need PGP signed keys
* You need to file a JIRA ticket with sonatype to have access to push there.

## To push a new jar
0. First ```git pull twitter master```
1. Do ```sbt publish```
2. Do ```sbt ++2.9.1 publish```
3. Visit [the Sonatype spaceship console](https://oss.sonatype.org/) ad log in.
4. Click on "staging repositories" to the left. Your jar should be there.
5. Click on each one of your jars and select close from the buttons at the top.
