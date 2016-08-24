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

package consumer.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import consumer.kafka.client.KafkaRangeReceiver;
import consumer.kafka.client.KafkaReceiver;

@SuppressWarnings("serial")
public class ReceiverLauncher implements Serializable {

  public static final Logger LOG = LoggerFactory
      .getLogger(ReceiverLauncher.class);
  private static String _zkPath;

  public static DStream<MessageAndMetadata> launch(
      StreamingContext ssc,
      Properties pros,
      int numberOfReceivers,
      StorageLevel storageLevel) {
    JavaStreamingContext jsc = new JavaStreamingContext(ssc);
    return createStream(jsc, pros, numberOfReceivers, storageLevel).dstream();
  }

  public static JavaDStream<MessageAndMetadata> launch(
      JavaStreamingContext jsc,
      Properties pros,
      int numberOfReceivers,
      StorageLevel storageLevel) {
    return createStream(jsc, pros, numberOfReceivers, storageLevel);
  }

  private static JavaDStream<MessageAndMetadata> createStream(
      JavaStreamingContext jsc,
      Properties pros,
      int numberOfReceivers,
      StorageLevel storageLevel) {
    int numberOfPartition;
    List<JavaDStream<MessageAndMetadata>> streamsList =
        new ArrayList<JavaDStream<MessageAndMetadata>>();
    JavaDStream<MessageAndMetadata> unionStreams;
    KafkaConfig kafkaConfig = new KafkaConfig(pros);
    ZkState zkState = new ZkState(kafkaConfig);
    String numberOfPartitionStr = (String) pros.getProperty(Config.KAFKA_PARTITIONS_NUMBER);
    if (numberOfPartitionStr != null) {
      numberOfPartition = Integer.parseInt(numberOfPartitionStr);
    } else {
      _zkPath = (String) kafkaConfig._stateConf.get(Config.ZOOKEEPER_BROKER_PATH);
      String _topic = (String) kafkaConfig._stateConf.get(Config.KAFKA_TOPIC);
      numberOfPartition = getNumPartitions(zkState, _topic);
    }

    // Create as many Receiver as Partition
    if (numberOfReceivers >= numberOfPartition) {
      for (int i = 0; i < numberOfPartition; i++) {
        streamsList.add(jsc.receiverStream(new KafkaReceiver(pros, i, storageLevel)));
      }
    } else {
      // create Range Receivers..
      Map<Integer, Set<Integer>> rMap = new HashMap<Integer, Set<Integer>>();
      for (int i = 0; i < numberOfPartition; i++) {
        int j = i % numberOfReceivers;
        Set<Integer> pSet = rMap.get(j);
        if (pSet == null) {
          pSet = new HashSet<Integer>();
          pSet.add(i);
        } else {
          pSet.add(i);
        }
        rMap.put(j, pSet);
      }
      for (int i = 0; i < numberOfReceivers; i++) {
        streamsList.add(jsc.receiverStream(new KafkaRangeReceiver(pros, rMap.get(i), storageLevel)));
      }
    }

    // Union all the streams if there is more than 1 stream
    if (streamsList.size() > 1) {
      unionStreams = jsc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
    } else {
      // Otherwise, just use the 1 stream
      unionStreams = streamsList.get(0);
    }

    boolean backPressureEnabled = (boolean) kafkaConfig._backpressureEnabled;
    if (backPressureEnabled) {
      initializeLisnter(jsc, kafkaConfig, numberOfPartition);
    }
    return unionStreams;
  }

  private static void initializeLisnter(
      JavaStreamingContext jsc,
        KafkaConfig kafkaConfig,
        int numberOfPartition) {

    final int DEFAULT_RATE = kafkaConfig._fetchSizeBytes;
    final int MIN_RATE = kafkaConfig._minFetchSizeBytes;
    final int fillFreqMs = kafkaConfig._fillFreqMs;
    final KafkaConfig config = kafkaConfig;
    final int partitionCount = numberOfPartition;
    final PIDController controller =
        new PIDController(
            kafkaConfig._proportional,
              kafkaConfig._integral,
              kafkaConfig._derivative);
    final long batchDuration = jsc.ssc().graph().batchDuration().milliseconds();
    jsc.addStreamingListener(new StreamingListener() {

      @Override
      public void onReceiverStopped(StreamingListenerReceiverStopped arg0) {
      }
      @Override
      public void onReceiverStarted(StreamingListenerReceiverStarted arg0) {
      }
      @Override
      public void onReceiverError(StreamingListenerReceiverError arg0) {
      }
      @Override
      public void onBatchSubmitted(StreamingListenerBatchSubmitted arg0) {
      }
	    @Override
      public void onOutputOperationStarted(StreamingListenerOutputOperationStarted arg0) {
      }
      @Override
      public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted arg0) {
      }
      @Override
      public void onBatchStarted(StreamingListenerBatchStarted arg0) {
      }

      @Override
      public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
        long processingDelay = (Long) batchCompleted.batchInfo().processingDelay().get();
        long schedulingDelay = (Long) batchCompleted.batchInfo().schedulingDelay().get();
        int batchFetchSize = DEFAULT_RATE;
        ZkState state = new ZkState((String) config._stateConf.get(Config.ZOOKEEPER_CONSUMER_CONNECTION));

        int newRate = controller.calculateRate(
            System.currentTimeMillis(),
            batchDuration,
            partitionCount,
            batchFetchSize,
            fillFreqMs,
            schedulingDelay,
            processingDelay);

        // Setting to Min Rate
        if (newRate <= 0) {
          newRate = MIN_RATE;
        }

        String path = ratePath();
        Map<Object, Object> metadata = (Map<Object, Object>) ImmutableMap.builder()
              .put("consumer",ImmutableMap.of("id", config._stateConf.get("kafka.consumer.id")))
              .put("topic", config._stateConf.get("kafka.topic"))
              .put("rate", newRate)
              .build();
        state.writeJSON(path, metadata);
        state.close();
      }

      public String ratePath() {
        return config._stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH)
            + "/" + config._stateConf.get(Config.KAFKA_CONSUMER_ID)
            + "/" + config._stateConf.get(Config.KAFKA_TOPIC) + "/newrate";
      }
    });
  }

  private static int getNumPartitions(ZkState zkState, String topic) {
    try {
      String topicBrokersPath = partitionPath(topic);
      List<String> children = zkState.getCurator().getChildren().forPath(topicBrokersPath);
      return children.size();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String partitionPath(String topic) {
    return _zkPath + "/topics/" + topic + "/partitions";
  }
}
