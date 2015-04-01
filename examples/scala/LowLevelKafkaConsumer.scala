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
      .setMaster("spark://10.252.5.113:7077")
      .setAppName("LowLevelKafkaConsumer")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.streaming.unpersist", "true")
      .set("spark.streaming.blockInterval", "200")

    val sc = new SparkContext(conf)

    //Might want to uncomment and add the jars if you are running on standalone mode.
    sc.addJar("/home/kafka-spark-consumer/target/kafka-spark-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
    val ssc = new StreamingContext(sc, Seconds(10))

    val topic = "valid_subpub"
    val zkhosts = "10.252.5.131"
    val zkports = "2181"
    val brokerPath = "/brokers"
	
	//Specify number of Receivers you need. 
	//It should be less than or equal to number of Partitions of your topic
    val numberOfReceivers = 1

	//The number of partitions for the topic will be figured out automatically
	//However, it can be manually specified by adding kafka.partitions.number property
    val kafkaProperties: Map[String, String] = Map("zookeeper.hosts" -> zkhosts,
                                                   "zookeeper.port" -> zkports,
                                                   "zookeeper.broker.path" -> brokerPath ,
                                                   "kafka.topic" -> topic,
                                                   "zookeeper.consumer.connection" -> "10.252.5.113:2182",
                                                   "zookeeper.consumer.path" -> "/spark-kafka",
                                                   "kafka.consumer.id" -> "12345")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}
	
	val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers)

    tmp_stream.foreachRDD(rdd => println("\n\nNumber of records in this batch : " + rdd.count()))

    ssc.start()
    ssc.awaitTermination()


  }

}

