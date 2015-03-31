/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consumer.kafka.client;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ReceiverLauncher;

public class Consumer implements Serializable {

	private static final long serialVersionUID = 4332618245650072140L;
	
	public void start() throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		run();
	}

	private void run() {

		
		Properties props = new Properties();
		props.put("zookeeper.hosts", "10.252.5.131");
		props.put("zookeeper.port", "2181");
		props.put("zookeeper.broker.path", "/brokers");
		props.put("kafka.topic", "valid_subpub");
		props.put("kafka.consumer.id", "valid_subpub");		
		props.put("zookeeper.consumer.connection", "10.252.5.113:2182");
		props.put("zookeeper.consumer.path", "/kafka-new");
		
		SparkConf _sparkConf = new SparkConf().setAppName("KafkaReceiver")
				.set("spark.streaming.receiver.writeAheadLog.enable", "false");;

		JavaStreamingContext ssc = new JavaStreamingContext(_sparkConf,
				new Duration(10000));
		
		//Specify number of Receivers you need. 
		//It should be less than or equal to number of Partitions of your topic
		
		int numberOfReceivers = 2;

		JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(ssc, props, numberOfReceivers);

		unionStreams
				.foreachRDD(new Function2<JavaRDD<MessageAndMetadata>, Time, Void>() {

					@Override
					public Void call(JavaRDD<MessageAndMetadata> rdd,
							Time time) throws Exception {

						System.out.println("Number of records in this Batch is " + rdd.count());
						return null;
					}
				});
		
		ssc.start();
		ssc.awaitTermination();
	}

	public static void main(String[] args) throws Exception {

		Consumer consumer = new Consumer();
		consumer.start();
	}
}
