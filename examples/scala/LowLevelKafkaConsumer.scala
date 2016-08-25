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
      .setMaster("spark://x.x.x.x:7077")
      .setAppName("LowLevelKafkaConsumer")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.streaming.unpersist", "true")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val topic = "xxxx"
    val zkhosts = "x.x.x.x"
    val zkports = "2181"
    val brokerPath = "/brokers"

    //Specify number of Receivers you need. 
    val numberOfReceivers = 1

    val kafkaProperties: Map[String, String] = 
	Map("zookeeper.hosts" -> zkhosts,
        "zookeeper.port" -> zkports,
        "zookeeper.broker.path" -> brokerPath ,
        "kafka.topic" -> topic,
        "zookeeper.consumer.connection" -> "x.x.x.x:2181",
        "zookeeper.consumer.path" -> "/spark-kafka",
        "kafka.consumer.id" -> "kafka-consumer",
        //optional properties
        "consumer.forcefromstart" -> "true",
        "consumer.backpressure.enabled" -> "true",
        "consumer.fetchsizebytes" -> "1048576",
        "consumer.fillfreqms" -> "250",
        "consumer.num_fetch_to_buffer" -> "1")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}

    val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers,StorageLevel.MEMORY_ONLY)
    //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
    val partitonOffset_stream = ProcessedOffsetManager.getPartitionOffset(tmp_stream)

    //Start Application Logic
    tmp_stream.foreachRDD(rdd => {
        println("\n\nNumber of records in this batch : " + rdd.count())
    } )
    //End Application Logic

    //Persists the Max Offset of given Kafka Partition to ZK
    ProcessedOffsetManager.persists(partitonOffset_stream, props)
    ssc.start()
    ssc.awaitTermination()


  }

}

