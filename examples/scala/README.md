# Lowlevel Kafka Consumer Scala Example

Scala example for the Lowlevel Kafka Consumer using SparkStreaming.

### How to Run
Follow the below steps to get it running:
```sh
* $ git clone https://github.com/dibbhatt/kafka-spark-consumer.git
* $ cd kafka-spark-consumer
* $ mvn install
* $ cp target/kafka-spark-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar examples/scala/lib/
* $ cd examples/scala/
* $ sbt package
* $ sbt run
```
By default, the program runs in **local mode** and connects to local kafka instance (zookeeper) running on **localhost:2181**,  reads from the topic **topic_with_3_partitions** and prints the count of number of records read and converts the Array[Byte] to String and prints the messages in the console.


