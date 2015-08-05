# Scalding on Amazon Elastic MapReduce

I copied this from the Google Group Discussion about how to get Scalding running in EMR:

I was able to successfully execute the WordCountJob Scalding's example on Amazon EMR.  To recap, here are the steps I took:

- I excluded Hadoop from the scalding assembly by changing the last lines in build.sbt:

    ```scala
    excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      cp filter { Set("janino-2.5.16.jar", "hadoop-core-0.20.2.jar") contains _.data.getName}
    }
    ```

- I uploaded the resulting scalding jar and the hello.txt file to Amazon S3
- I created an EMR job using a custom jar. From the command line, it looks like this:

    ```bash
    elastic-mapreduce --create --name "Test Scalding" --jar s3n://<bucket-and-path-to-scalding-assembly-0.3.5.jar> --arg com.twitter.scalding.examples.WordCountJob --arg --hdfs --arg --input --arg s3n://<bucket-and-path-to-hello.txt> --arg --output --arg s3n://<bucket-and-path-to-output-dir>
    ```

Voilà! :-)

For an example of a standalone Scalding job which can be run on Amazon EMR, please see:

[https://github.com/snowplow/scalding-example-project](https://github.com/snowplow/scalding-example-project)
