retrieveManaged := true
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
name := "LowlevelKafkaConsumer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0"
libraryDependencies += "dibbhatt" % "kafka-spark-consumer" % "1.0.8"
libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.10.0.0"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
