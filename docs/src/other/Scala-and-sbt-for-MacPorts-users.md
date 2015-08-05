# Scala and SBT for Homebrew users

As of this writing, MacPorts (2.1.2) installs sbt 0.12, which is not supported by either master or develop.  Applying this patch was found to work:

    diff --git a/build.sbt b/build.sbt
    index 4363ee8..e19a2de 100644
    --- a/build.sbt
    +++ b/build.sbt
    @@ -43,6 +43,8 @@ excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
     // Some of these files have duplicates, let's ignore:
     mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
       {
    -    case _ => MergeStrategy.last // leiningen build files
    +    case s if s.endsWith(".class") => MergeStrategy.last
    +    case s if s.endsWith("project.clj") => MergeStrategy.concat
    +    case x => old(x)
       }
     }
    diff --git a/project/plugins.sbt b/project/plugins.sbt
    index 6876bc2..c5086e1 100644
    --- a/project/plugins.sbt
    +++ b/project/plugins.sbt
    @@ -1,6 +1,3 @@
    -addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.1")
    -
    -resolvers += Resolver.url("sbt-plugin-releases",
    -  url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
    -
    +resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

    +addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.3")

You will also need to manually patch /opt/local/bin/sbt or set JAVA_OPTS to (recommendation from master sbt code site):

    JAVA_OPTS="-Xms512M -Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M"
