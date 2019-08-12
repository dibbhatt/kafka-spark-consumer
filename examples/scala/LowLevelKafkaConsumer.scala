import consumer.kafka.{ProcessedOffsetManager, ReceiverLauncher}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext};

object LowLevelKafkaConsumer {

  def main(arg: Array[String]): Unit = {

    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Create SparkContext
    val conf = new SparkConf()
      .setMaster("spark://localhost:7077")
      .setAppName("LowLevelKafkaConsumer")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.streaming.unpersist", "true")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val topic = "mytopic"
    val zkhosts = "localhost"
    val zkports = "2181"

    //Specify number of Receivers you need.
    val numberOfReceivers = 1

    val kafkaProperties: Map[String, String] =
	Map("zookeeper.hosts" -> zkhosts,
        "zookeeper.port" -> zkports,
        "kafka.topic" -> topic,
        "zookeeper.consumer.connection" -> "localhost:2181",
        "kafka.consumer.id" -> "kafka-consumer",
        //optional properties
        "consumer.forcefromstart" -> "false",
        "consumer.backpressure.enabled" -> "true",
        "max.poll.records" -> "100",
        "consumer.fillfreqms" -> "1000",
        "consumer.num_fetch_to_buffer" -> "5")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}

    val unionStreamsm = ReceiverLauncher.launch(ssc, props, numberOfReceivers,StorageLevel.MEMORY_ONLY)
    
    unionStreams.foreachRDD(rdd => {
        //Start Application Logic e.g. rdd.foreachPartition
        println("\n\nNumber of records in this batch : " + rdd.count())
        //End Application Logic

        //Persists the Max Offset of given Kafka Partition to ZK
        ProcessedOffsetManager.persistsPartition(rdd, props)

    } )

    ssc.start()
    ssc.awaitTermination()


  }

}
