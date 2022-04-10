resolvers ++= Seq(
  "jgit-repo".at("https://download.eclipse.org/jgit/maven"),
  "sonatype-releases".at("https://oss.sonatype.org/content/repositories/releases"),
  "Twitter Maven".at("https://maven.twttr.com")
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.47deg" % "sbt-microsites" % "1.3.4")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.10")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addSbtPlugin("com.twitter" %% "scrooge-sbt-plugin" % "18.9.0")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.14")
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.3")
addSbtPlugin("com.github.sbt" % "sbt-jacoco" % "3.4.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.7")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.16")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.2.22")
