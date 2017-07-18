retrieveManaged := true
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
name := "LowlevelKafkaConsumer"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"
libraryDependencies += "dibbhatt" % "kafka-spark-consumer" % "1.0.11"
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.1.1"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
