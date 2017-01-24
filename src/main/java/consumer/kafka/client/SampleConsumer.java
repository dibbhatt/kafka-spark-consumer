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
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;

@SuppressWarnings("serial")
public class SampleConsumer implements Serializable {

  public void start() throws InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    run();
  }

  @SuppressWarnings("deprecation")
  private void run() {

    Properties props = new Properties();
    props.put("zookeeper.hosts", "localhost");
    props.put("zookeeper.port", "2181");
    props.put("zookeeper.broker.path", "/brokers");
    props.put("kafka.topic", "mytopic");
    props.put("kafka.consumer.id", "kafka-consumer");
    props.put("zookeeper.consumer.connection", "localhost:2181");
    props.put("zookeeper.consumer.path", "/spark-kafka");
    // Optional Properties
    props.put("consumer.forcefromstart", "false");
    props.put("consumer.fetchsizebytes", "1048576");
    props.put("consumer.fillfreqms", "1000");
    props.put("consumer.backpressure.enabled", "true");
    props.put("consumer.num_fetch_to_buffer", "1");
    props.put("kafka.message.handler.class",
          "consumer.kafka.IdentityMessageHandler");

    SparkConf _sparkConf = new SparkConf();
    JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, Durations.seconds(30));
    // Specify number of Receivers you need.
    int numberOfReceivers = 1;

    JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(
        jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());

    //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
    JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
        .getPartitionOffset(unionStreams);

    //Start Application Logic
    unionStreams.foreachRDD(new Function<JavaRDD<MessageAndMetadata>, Void>() {
      @Override
      public Void call(JavaRDD<MessageAndMetadata> rdd) throws Exception {
        List<MessageAndMetadata> rddList = rdd.collect();
        System.out.println(" Number of records in this batch " + rddList.size());
        return null;
      }
    });
    //End Application Logic

    //Persists the Max Offset of given Kafka Partition to ZK
    ProcessedOffsetManager.persists(partitonOffset, props);
    jsc.start();
    jsc.awaitTermination();
  }

  public static void main(String[] args) throws Exception {
    SampleConsumer consumer = new SampleConsumer();
    consumer.start();
  }
}
