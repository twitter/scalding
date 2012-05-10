addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.1")

resolvers += Resolver.url("sbt-plugin-releases",
  new
  URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)


