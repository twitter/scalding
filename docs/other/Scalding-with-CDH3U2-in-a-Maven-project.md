# Scalding with CDH3U2 in a Maven Project

## Introduction

### Aim
This wiki describes a procedure that should allow the dedicated reader to create an executable jar file implementing Scalding, using Maven, that is readily available for deployment on CDH3U2 cluster.

### Hadoop Flavors and Compatibility Issues
To deploy a MapReduce job on any Hadoop cluster, since the different Hadoop versions are not necessarily compatible with each other, one has to ensure that the core Hadoop libraries the client code uses are identical to those found throughout the entire cluster. Roughly said, client code that is planned to be deployed as an executable jar, should use the same exact jars as are used by the server nodes on the cluster. See http://www.cloudera.com/blog/2012/01/an-update-on-apache-hadoop-1-0/ for a walk down the Hadoop and Cloudera version road of chaos.

## Protocol

### Prerequisites
* Scalding source - here we used v0.5.3
* SBT - to build Scalding
* Cloudera's Hadoop (CDH) - binaries are fine, e.g. hadoop-0.20.2-cdh3u2.tar.gz . Other versions are cool, just use the same version your cluster uses.
* IDE with Maven support - here I use Eclipse. There is no need for an IDE if you are a Maven wizard. I am not one of those.

### Procedure
1. CD to your Scalding source directory
2. Edit build.sbt to exclude the hadoop-core jar from being packaged in Scalding:

    ```scala
    excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      cp.filter { Set("janino-2.5.16.jar", "hadoop-core-0.20.2.jar") contains _.data.getName }
    }
    ```

    (https://gist.github.com/238d74b081d9f2c6e5f1)
3. `sbt -29 update` (-29 is a flag for SBT to build with Scala 2.9.1 libraries. Use if you intend to implement your code with this version of Scala)
4. `sbt -29 assembly` (creates scalding-assembly.0.5.3.jar)
5. My own preference is to install self compiled jars in my local Maven repository. Therefore I use mvn install:install-file target (see http://maven.apache.org/plugins/maven-install-plugin/usage.html) to install the created scalding-assembly.0.5.3.jar locally. From hereon this jar's spec are `groupId=com.twitter artifactId=scalding-assembly version=0.5.3.cdh3u2`
6. Download Cloudera's hadoop-0.20.2-cdh3u2.tar.gz
7. As in 5, install locally your hadoop-core-cdh3u2.jar, or alternatively you can embed Cloudera's parent pom in your project's pom (in the following steps) - they have instructions somewhere on their website)
8. In your IDE, create a new Scala project using/based on this pom: https://gist.github.com/40f1838bbdd15cc25b21
9. Create the file src/assembly/job.xml and edit: https://gist.github.com/9c5e6f04da287667983a
10. The fun part! - Create your Scala class implementing Scalding's Job
    ```scala
    class SomethingCool(args: Args) extends Job(args)
    ```
11. `mvn package` (creates a fat jar)
12. The generated jar would be placed under your project's target folder, named like: `YOURPROJECT-0.0.1-SNAPSHOT-job.jar`
13. CD to your hadoop-0.20-cdh3u2 folder
14. Setup your Hadoop configuration files (most importantly, your `conf/core-site.xml` file) and edit
    ```xml
    <property>
      <name>fs.default.name</name>
      <value>hdfs://namenode.somethingcool.com:8020/</value>
    </property>
    ```

15. Run

    ```bash
    bin/hadoop jar YOURPROJECT-0.0.1-SNAPSHOT-job.jar com.twitter.scalding.Tool your.package.your.class --hdfs --input` `hdfs://namenode.somethingcool.com/user/hdfs/tmp/hello.txt --output hdfs://namenode.somethingcool.com/user/hdfs/tmp/hello_out.txt
    -libjars YOURPROJECT-0.0.1-SNAPSHOT-job.jar
    ```



