name := "FanMonitor"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.5"

organization := "devicehive.com"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.3.1",
  "org.scalanlp" %% "breeze" % "0.11.1" % "provided",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2",
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

assemblyJarName in assembly := "fanmonitor.jar"

assemblyMergeStrategy in assembly := {
  case PathList(p @ _*) if p.last endsWith "UnusedStubClass.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
