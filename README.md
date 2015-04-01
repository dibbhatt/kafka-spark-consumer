README file for Kafka-Spark-Consumer
===================================

NOTE : This Kafka Spark Consumer code is taken from Kafka spout of the Apache Storm project (https://github.com/apache/storm/tree/master/external/storm-kafka), 
which was originally created by wurstmeister (https://github.com/wurstmeister/storm-kafka-0.8-plus).
Original Storm Kafka Spout Code has been modified to work with Spark Streaming.

This utility will help to pull messages from Kafka Cluster using Spark Streaming.
The Kafka Consumer is Low Level Kafka Consumer ( SimpleConsumer) and have better handling of the Kafka Offsets and handle failures.

This code have implemented a Custom Receiver which uses low level Kafka Consumer API to fetch messages from Kafka and 'store' it in Spark BlockManager.

The logic will automatically detect number of partitions for a topic and spawn as many Kafka Receivers based on your configured number of Receivers.

e.g. if you have 100 partitions of a Topic, and you need 20 Receivers, each Receiver will handle 5 partition. 

Number of Receivers should be less than or equal to the number of Partitions for Kafka Topic.

In your driver code , you can launch the Receivers by calling ReceiverLauncher.launch

Please see Java or Scala code example on how to use this Low Level Consumer

Kafka Receivers uses Zookeeper for storing the latest offset for individual partitions, which will help to recover in case of failure .


Following are the instructions to build 
========================================

>git clone

>cd kafka-spark-consumer

>mvn install

Integrating Spark-Kafka-Consumer
=================================

If you want to use this Kafka-Spark-Consumer for you target client application, include below dependency in your pom.xml

                <dependency>
                        <groupId>kafka.spark.consumer</groupId>
                        <artifactId>kafka-spark-consumer</artifactId>
                        <version>0.0.1-SNAPSHOT</version>
                </dependency>

				
and use below properties. ( See Java and Scala Code example on how to use these properties)

* Kafka ZK details from where messages will be pulled. Speficy ZK Host IP address
	* zookeeper.hosts=host1,host2
* Kafka ZK Port
	* zookeeper.port=2181
* Kafka Broker path in ZK
	* zookeeper.broker.path=/brokers
* Kafka Topic to consume
	* kafka.topic=topic-name

* Consumer ZK Path. This will be used to store the consumed offset. Please specify correct ZK IP and Port
	* zookeeper.consumer.connection=x.x.x.x:2181
* ZK Path for storing Kafka Consumer offset
	* zookeeper.consumer.path=/spark-kafka
* Kafka Consumer ID. This ID will be used for accessing offset details in $zookeeper.consumer.path
	* kafka.consumer.id=12345
* Number of partitions for the topic. Optional. Only required if ZK is not reachable from the driver.
	* kafka.partitions.number=100


Java Example
============

		Properties props = new Properties();
		props.put("zookeeper.hosts", "x.x.x.x");
		props.put("zookeeper.port", "2181");
		props.put("zookeeper.broker.path", "/brokers");
		props.put("kafka.topic", "some-topic");
		props.put("kafka.consumer.id", "consumer-id");		
		props.put("zookeeper.consumer.connection", "x.x.x.x:2181");
		props.put("zookeeper.consumer.path", "/consumer-path");
		
		SparkConf _sparkConf = new SparkConf().setAppName("KafkaReceiver")
				.set("spark.streaming.receiver.writeAheadLog.enable", "false");;

		JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf,
				new Duration(10000));
		
		//Specify number of Receivers you need. 
		//It should be less than or equal to number of Partitions of your topic
		
		int numberOfReceivers = 3;

		JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(jsc, props, numberOfReceivers);

		unionStreams
				.foreachRDD(new Function2<JavaRDD<MessageAndMetadata>, Time, Void>() {

					@Override
					public Void call(JavaRDD<MessageAndMetadata> rdd,
							Time time) throws Exception {

						System.out.println("Number of records in this Batch is " + rdd.count());
						return null;
					}
				});
		
		jsc.start();
		jsc.awaitTermination();
		

		Complete example is available here : 

		The src/main/java/consumer/kafka/client/Consumer.java is the sample Java code which uses this ReceiverLauncher to generate DStreams from Kafka and apply a Output operation for every messages of the RDD.

		
Scala Example
=============


	val conf = new SparkConf()
	.setMaster("spark://x.x.x.x:7077")
	.setAppName("LowLevelKafkaConsumer")

    val sc = new SparkContext(conf)

    //Might want to uncomment and add the jars if you are running on standalone mode.
    //sc.addJar("/home/kafka-spark-consumer/target/kafka-spark-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
	
    val ssc = new StreamingContext(sc, Seconds(10))

    val topic = "some-topic"
    val zkhosts = "x.x.x.x"
    val zkports = "2181"
    val brokerPath = "/brokers"
	
	//Specify number of Receivers you need. 
	//It should be less than or equal to number of Partitions of your topic
    val numberOfReceivers = 3

    val kafkaProperties: Map[String, String] = Map("zookeeper.hosts" -> zkhosts,
                                                   "zookeeper.port" -> zkports,
                                                   "zookeeper.broker.path" -> brokerPath ,
                                                   "kafka.topic" -> topic,
                                                   "zookeeper.consumer.connection" -> "x.x.x.x:2181",
                                                   "zookeeper.consumer.path" -> "/consumer-path",
                                                   "kafka.consumer.id" -> "consumer-id")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}
	
	val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers)

    tmp_stream.foreachRDD(rdd => println("\n\nNumber of records in this batch : " + rdd.count()))

    ssc.start()
    ssc.awaitTermination()
	
	Complete example is available here :
	
	examples/scala/LowLevelKafkaConsumer.scala is a sample scala code on how to use this utility.

	
Some Tuning Options
===================

The Low Level Kafka Consumer consumes messages from Kafka in Rate Limiting way. Default settings can be found in consumer.kafka.KafkaConfig.java class

You can see following two variables

	public int _fetchSizeBytes = 512 * 1024;
	public int _fillFreqMs = 200 ;
	
This suggests that, Receiver for any given Partition of a Topic will pull 512 KB Block of data at every 200ms.
With this default settings, let assume your Kafka Topic have 5 partitions, and your Spark Batch Duration is say 10 Seconds, this Consumer will pull

512 KB x ( 10 seconds / 200 ms ) x 5 = 128 MB of data for every Batch.

If you need higher rate, you can increase the _fetchSizeBytes , or if you need less number of Block generated you can increase _fillFreqMs.

These two parameter need to be carefully tuned keeping in mind your downstream processing rate and your memory settings.

Once you change these settings, you need to rebuild kafka-spark-consumer.

Note : If you need more finer control of your Receivers, you can directly use KafkaReceiver or KafkaRangeReceiver based on your use case like you want to consume from ONLY one Partition , or you want to consume from 
SUBSET of partition . 

Running Spark Kafka Consumer
===========================
Let assume your Kafka Message Processing logic is in custom-processor.jar which is built using the spark-kafka-consumer as dependency.

Launch this using spark-submit

./bin/spark-submit --class x.y.z.YourDriver --master spark://x.x.x.x:7077 --executor-memory 5G /<Path_To>/custom-processor.jar

This will start the Spark Receiver and Fetch Kafka Messages for every partition of the given topic and generates the DStream.

e.g. to Test Consumer provided in the package with your Kafka settings please modify it to point to your Kafka and use below command for spark submit 

./bin/spark-submit --class consumer.kafka.client.Consumer --master spark://x.x.x.x:7077 --executor-memory 5G /<Path_To>/kafka-spark-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar


 
