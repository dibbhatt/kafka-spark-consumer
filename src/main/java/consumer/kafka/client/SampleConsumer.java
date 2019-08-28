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
import java.util.Iterator;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;

@SuppressWarnings("serial")
public class SampleConsumer implements Serializable {

    private static final Logger LOG = LoggerFactory
            .getLogger(SampleConsumer.class);

  public void start() throws InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    run();
  }

  @SuppressWarnings("deprecation")
  private void run() {

    Properties props = new Properties();
    props.put("zookeeper.hosts", "zkhost");
    props.put("zookeeper.port", "2181");
    props.put("kafka.topic", "topicA,topicB,topicC");
    props.put("kafka.consumer.id", "kafka-consumer");
    // Optional Properties
    props.put("zookeeper.broker.path", "/brokers");
    props.put("zookeeper.consumer.path", "/consumers");
    props.put("consumer.forcefromstart", "false");
    props.put("max.poll.records", "10");
    props.put("consumer.fillfreqms", "500");
    props.put("consumer.backpressure.enabled", "true");
    //Kafka properties
    props.put("bootstrap.servers", "kafkahost-1:6667,"
            + "kafkahost-2:6667,"
            + "kafkahost-3:6667,"
            + "kafkahost-4:6667");
    props.put("security.protocol", "SSL");
    props.put("ssl.truststore.location","~/kafka-securitykafka.server.truststore.jks");
    props.put("ssl.truststore.password", "test1234");

    SparkConf _sparkConf = new SparkConf();
    JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, Durations.seconds(30));
    // Specify number of Receivers you need.
    int numberOfReceivers = 6;

    JavaDStream<MessageAndMetadata<byte[]>> unionStreams = ReceiverLauncher.launch(
        jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());

    unionStreams.foreachRDD(new VoidFunction<JavaRDD<MessageAndMetadata<byte[]>>>() {
      @Override
      public void call(JavaRDD<MessageAndMetadata<byte[]>> rdd) throws Exception {
        //Start Application Logic
        rdd.foreachPartition(new VoidFunction<Iterator<MessageAndMetadata<byte[]>>>() {
            @Override
            public void call(Iterator<MessageAndMetadata<byte[]>> mmItr) throws Exception {
                int countTopicA = 0;
                int countTopicB = 0;
                int countTopicC = 0;
                while(mmItr.hasNext()) {
                    MessageAndMetadata<byte[]> mm = mmItr.next();
                    if(mm.getTopic().equals("topicA")) {
                        countTopicA++;
                    }
                    else if (mm.getTopic().equals("topicB")) {
                        countTopicB++;
                    }
                    else if (mm.getTopic().equals("topicC")) {
                        countTopicC++;
                    }
                }
                System.out.println("topicA count " + countTopicA);
                System.out.println("topicB count " + countTopicB);
                System.out.println("topicC count " + countTopicC);
            }
        });
        System.out.println("RDD count " + rdd.count());
        //End Application Logic
        //commit offset
        System.out.println("Commiting Offset");
        ProcessedOffsetManager.persistsPartition(rdd, props);
      }
    });

    try {
      jsc.start();
      jsc.awaitTermination();
    }catch (Exception ex ) {
      jsc.ssc().sc().cancelAllJobs();
      jsc.stop(true, false);
      System.exit(-1);
    }
  }

  public static void main(String[] args) throws Exception {
    SampleConsumer consumer = new SampleConsumer();
    consumer.start();
  }
}
