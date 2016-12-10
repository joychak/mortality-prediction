name := "cse8803_project"
version := "1.0"
scalaVersion := "2.11.8"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.rogach"          % "scallop_2.11"          % "2.0.0",
  "org.apache.spark"    % "spark-core_2.11"       % "2.0.0" % "provided",
  "org.apache.spark"    % "spark-mllib_2.11"      % "2.0.0" % "provided",
  "org.apache.spark"    % "spark-graphx_2.11"     % "2.0.0" % "provided",
  "com.google.protobuf" % "protobuf-java"         % "2.6.1"
  //"org.sameersingh.scalaplot" % "scalaplot"       % "0.0.4"
)

//libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.1"
parallelExecution in Test := false

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)
publishArtifact := true
