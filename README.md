# README file for Kafka-Spark-Consumer


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

#What is Different from Spark Out of Box Kafka Consumers

This Consumer is Receiver based fault tolerant reliable consumer designed using Kafka Low Level Simple Consumer API. This Receiver is designed to recover from any underlying failure .

1. Spark's Out of Box Receiver based consumer i.e. KafkaUtil CreateStream uses Kafka High Level API which has serious issue with Consumer Re-balance and hence can not be used in Production scenarios. 

2. This Consumer have its own mechanism to create Block from Kafka Stream ( See more details in "Some Tuning Options" section below ) and write Blocks to Spark BlockManager. This consumer implemented the Rate Limiting logic not by controlling the number of messages per block ( as it is done in Spark's Out of Box Kafka Consumers), but by size of the blocks per batch. i.e. for any given batch, this consumer controls the Rate limit by controlling the size of the batches. As Spark memory is driven by block size rather the number of messages I think rate limit by block size is more appropriate. 

e.g. Let assume Kafka  contains messages of very small sizes ( say few hundred bytes ) to larger messages ( to few hundred KB ) for same topic. Now if we control the rate limit by number of messages, Block sizes may vary drastically based on what type of messages get pulled . Whereas , if I control my rate limiting by size of block, my block size remain constant across batches (even though number of messages differ across blocks ) and can help to tune my memory settings more correctly as I know how much exact memory my Block is going to consume.  

3. This Consumer has its own PID (Proportional, Integral, Derivative ) Controller built into the consumer and control the Spark Back Pressure by modifying the size of Block it can consume at run time. The PID Controller rate feedback mechanism is built using Zookeeper. Again the logic to control Back Pressure is not by controlling number of messages ( as it is done in Spark 1.5 , SPARK-7398) but altering size of the Block consumed per batch from Kafka. As the Back Pressure is built into the Consumer, this consumer can be used with any version of Spark if anyone want to have a back pressure controlling mechanism in their existing Spark / Kafka environment. 

Even though the Spark DirectStream API uses the Kafka SimpleConsumer API, but as the Spark's back pressure logic (SPARK-7398) in Spark 1.5 is built by controlling the number of messages , it may so happen that number of messages consumed during every read from Kafka in DirectStream is much higher than the computed rate by Controller , but as it applies the clamp after pulling the messages from Kafka , DirectStream can unnecessarily pull extra messages and then applies the throttle to it . which add to unnecessary network I/O from Kafka broker to the spark executor machines. 

In This consumer , it controls the back-pressure directly by modifying the amount of data it can pull from Kafka and not by applying throttle after pulling large amount of data and then clamp it, and hence saves unnecessary network I/O.

If anyone use This Kafka Consumer , please refer "Spark Consumer Properties" section on how to enable back pressure. Also see "Some Tuning Options"  section on how to tune PID Controller.

4. Spark DirectStream API is still experimental. If one need to use it in Production scenarios, they need to manage their own offsets in some external store as default offset management by Check-pointing is not reliable.

5. This Consumer is modified version of Storm-Kafka spout which has been running in many production cases for quite some time. Hence the various logic related to Kafka fail-over and connection management is already proven and tested in production.

6. This Consumer will give end to end No Data Loss guarantee once enabled with Spark WAL feature.

# Instructions for Manual build 

	git clone https://github.com/dibbhatt/kafka-spark-consumer

	cd kafka-spark-consumer

	mvn install

And Use Below Dependency in your Maven 

		<dependency>
				<groupId>kafka.spark.consumer</groupId>
				<artifactId>kafka-spark-consumer</artifactId>
				<version>1.0.4</version>
		</dependency>

# Accessing from Spark Packages


This Consumer is now part of Spark Packages : http://spark-packages.org/package/dibbhatt/kafka-spark-consumer

Spark Packages Release is built using Spark 1.4.1 and Kafka 0.8.2.1 .

Include this package in your Spark Applications using:

* spark-shell, pyspark, or spark-submit

	$SPARK_HOME/bin/spark-shell --packages dibbhatt:kafka-spark-consumer:1.0.4


* sbt

If you use the sbt-spark-package plugin, in your sbt build file, add:

	spDependencies += "dibbhatt/kafka-spark-consumer:1.0.4"

Otherwise,

	resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
			  
	libraryDependencies += "dibbhatt" % "kafka-spark-consumer" % "1.0.4"


* Maven

In your pom.xml, add:

	<dependencies>
	  <!-- list of dependencies -->
	  <dependency>
		<groupId>dibbhatt</groupId>
		<artifactId>kafka-spark-consumer</artifactId>
		<version>1.0.4</version>
	  </dependency>
	</dependencies>
	<repositories>
	  <!-- list of other repositories -->
	  <repository>
		<id>SparkPackagesRepo</id>
		<url>http://dl.bintray.com/spark-packages/maven</url>
	  </repository>
	</repositories>

		
# Spark Consumer Properties
				
These are the Consumer Properties need to be used in your Driver Code. ( See Java and Scala Code example on how to use these properties)

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
	
* OPTIONAL - Number of partitions for the topic. Only required if ZK is not reachable from the driver.
	* kafka.partitions.number=100
* OPTIONAL - Consumer Force From Start . Default Consumer Starts from Latest offset.
	* consumer.forcefromstart=true
* OPTIONAL - Consumer Fetch Size in Bytes . Default 512 Kilo Bytes ( 512 * 1024 Bytes ). See further explanation in Tuning Options section
	* consumer.fetchsizebytes=1048576
* OPTIONAL - Consumer Fill Frequence in MS . Default 200 milliseconds . See further explanation in Tuning Options section
	* consumer.fillfreqms=250
* OPTIONAL - Consumer Back Pressure Support. Default is false
	* consumer.backpressure.enabled=false
	

# Java Example


		Properties props = new Properties();
		props.put("zookeeper.hosts", "x.x.x.x");
		props.put("zookeeper.port", "2181");
		props.put("zookeeper.broker.path", "/brokers");
		props.put("kafka.topic", "some-topic");
		props.put("kafka.consumer.id", "12345");		
		props.put("zookeeper.consumer.connection", "x.x.x.x:2181");
		props.put("zookeeper.consumer.path", "/consumer-path");
		//Optional Properties
		props.put("consumer.forcefromstart", "true");
		props.put("consumer.fetchsizebytes", "1048576");
		props.put("consumer.fillfreqms", "250");
		props.put("consumer.backpressure.enabled", "true");
		
		SparkConf _sparkConf = new SparkConf().setAppName("KafkaReceiver")
				.set("spark.streaming.receiver.writeAheadLog.enable", "false");;

		JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf,
				new Duration(10000));
		
		//Specify number of Receivers you need. 
		//It should be less than or equal to number of Partitions of your topic
		
		int numberOfReceivers = 3;

		JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(jsc, props, numberOfReceivers,StorageLevel.MEMORY_ONLY());

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

		
# Scala Example



	val conf = new SparkConf()
	.setMaster("spark://x.x.x.x:7077")
	.setAppName("LowLevelKafkaConsumer")

    val sc = new SparkContext(conf)

    //Might want to uncomment and add the jars if you are running on standalone mode.
    //sc.addJar("/home/kafka-spark-consumer/target/kafka-spark-consumer-1.0.4-jar-with-dependencies.jar")
	
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
                                                   "kafka.consumer.id" -> "12345",
												   //optional properties
												   "consumer.forcefromstart" -> "true",
												   "consumer.backpressure.enabled" -> "true",
												   "consumer.fetchsizebytes" -> "1048576",
												   "consumer.fillfreqms" -> "250")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}
	
	val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers,StorageLevel.MEMORY_ONLY)

    tmp_stream.foreachRDD(rdd => println("\n\nNumber of records in this batch : " + rdd.count()))

    ssc.start()
    ssc.awaitTermination()
	
	Complete example is available here :
	
	examples/scala/LowLevelKafkaConsumer.scala is a sample scala code on how to use this utility.

	
# Some Tuning Options**


## Block Size Tuning :

The Low Level Kafka Consumer consumes messages from Kafka in Rate Limiting way. Default settings can be found in consumer.kafka.KafkaConfig.java class

You can see following two variables

	public int _fetchSizeBytes = 512 * 1024;
	public int _fillFreqMs = 200 ;
	
This suggests that, Receiver for any given Partition of a Topic will pull 512 KB Block of data at every 200ms.
With this default settings, let assume your Kafka Topic have 5 partitions, and your Spark Batch Duration is say 10 Seconds, this Consumer will pull

512 KB x ( 10 seconds / 200 ms ) x 5 = 128 MB of data for every Batch.

If you need higher rate, you can increase the _fetchSizeBytes , or if you need less number of Block generated you can increase _fillFreqMs.

These two parameter need to be carefully tuned keeping in mind your downstream processing rate and your memory settings.

You can control these two paramater by consumer.fetchsizebytes and consumer.fillfreqms settings mentioned above.
 

## PID Back-Pressure Rate Tuning

You can enable the BackPressure meachanism by setting *consumer.backpressure.enabled* to "true" in Properties used for ReceiverLauncher

The Default PID settings is as below.

Proportional = 0.75
Integral = 0.15
Derivative = 0

If you increase any or all of these , your damping factor will be higher. So if you want to lower the Consumer rate more than what is being achived by applying the default PID rate , you can increase these values.

You can control the PID values by settings the Properties below.

*consumer.backpressure.proportional*
*consumer.backpressure.integral*
*consumer.backpressure.derivative*


# Running Spark Kafka Consumer

Let assume your Driver code is in xyz.jar which is built using the spark-kafka-consumer as dependency.

Launch this using spark-submit

./bin/spark-submit --class x.y.z.YourDriver --master spark://x.x.x.x:7077 --executor-memory 5G /<Path_To>/xyz.jar

This will start the Spark Receiver and Fetch Kafka Messages for every partition of the given topic and generates the DStream.

e.g. to Test Consumer provided in the package with your Kafka settings please modify it to point to your Kafka and use below command for spark submit 

./bin/spark-submit --class consumer.kafka.client.Consumer --master spark://x.x.x.x:7077 --executor-memory 5G /<Path_To>/kafka-spark-consumer-1.0.4-jar-with-dependencies.jar


 
