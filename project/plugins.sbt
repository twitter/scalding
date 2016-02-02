resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers ++= Seq(
  "jgit-repo" at "http://download.eclipse.org/jgit/maven",
  "sonatype-releases"  at "https://oss.sonatype.org/content/repositories/releases",
  "Twitter Maven" at "http://maven.twttr.com"
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.10.2")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("com.twitter" %% "scrooge-sbt-plugin" % "3.20.0")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.8")
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.2")
addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0.BETA1")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.4")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.2")
