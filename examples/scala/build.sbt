retrieveManaged := true
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
name := "LowlevelKafkaConsumer"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "dibbhatt" % "kafka-spark-consumer" % "1.0.12"
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.11.0.0"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
