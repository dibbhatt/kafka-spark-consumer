import consumer.kafka.ReceiverLauncher
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by akhld on 11/12/14.
 */

object LowLevelKafkaConsumer {

  def main(arg: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Create SparkContext
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("LowLevelKafkaConsumer")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.unpersist", "true")
      .set("spark.streaming.blockInterval", "200")

    val sc = new SparkContext(conf)

    /* Might want to uncomment and add the jars if you are running on standalone mode.
    sc.addJar("/home/akhld/.ivy2/cache/org.apache.spark/spark-streaming-kafka_2.10/jars/spark-streaming-kafka_2.10-1.1.0.jar")
    sc.addJar("/home/akhld/.ivy2/cache/com.101tec/zkclient/jars/zkclient-0.3.jar")
    sc.addJar("/home/akhld/.ivy2/cache/com.yammer.metrics/metrics-core/jars/metrics-core-2.2.0.jar")
    sc.addJar("/home/akhld/.ivy2/cache/org.apache.kafka/kafka_2.10/jars/kafka_2.10-0.8.0.jar")
    sc.addJar("/home/akhld/benchmark/test-kafka/target/scala-2.10/pubmatic_2.10-1.0.jar")
    sc.addJar("/home/akhld/sigmoid/localcluster/codes/private/kafka-spark-consumer/target/kafka-spark-consumer-0.0.1-SNAPSHOT.jar")
    */

    val ssc = new StreamingContext(sc, Seconds(10))

    val topic = "topic_with_3_partitions"

    val zkhosts = "localhost"
    val zkports = "2181"
    val brokerPath = "/brokers"
	
	//Specify number of Receivers you need. 
	//It should be less than or equal to number of Partitions of your topic
    val numberOfReceivers = 3

    val kafkaProperties: Map[String, String] = Map("zookeeper.hosts" -> zkhosts,
                                                   "zookeeper.port" -> zkports,
                                                   "zookeeper.broker.path" -> brokerPath ,
                                                   "kafka.topic" -> topic,
                                                   "zookeeper.consumer.connection" -> "localhost:2181",
                                                   "zookeeper.consumer.path" -> "/spark-kafka",
                                                   "kafka.consumer.id" -> "12345")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}
	
	val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers)

    tmp_stream.foreachRDD(rdd => println("\n\nNumber of records in this batch : " + rdd.count()))

    //Lets convert the Array[Byte] to String
    val stream = tmp_stream.map(x => { val s = new String(x.getPayload); s })

    stream.print()

    ssc.start()
    ssc.awaitTermination()


  }

}

