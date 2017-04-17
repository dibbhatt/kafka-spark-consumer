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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consumer.kafka.client.KafkaRangeReceiver;
import consumer.kafka.client.KafkaReceiver;

@SuppressWarnings("serial")
public class ReceiverLauncher implements Serializable {
    public static final Logger LOG = LoggerFactory.getLogger(ReceiverLauncher.class);

    private static String _zkPath;

    public static <E> DStream<MessageAndMetadata<E>> launch(
            StreamingContext ssc, Properties pros, int numberOfReceivers,
            StorageLevel storageLevel, KafkaMessageHandler<E> messageHandler) {
        JavaStreamingContext jsc = new JavaStreamingContext(ssc);
        return createStream(jsc, pros, numberOfReceivers, storageLevel, messageHandler).dstream();
    }

    public static DStream<MessageAndMetadata<byte[]>> launch(
        StreamingContext ssc, Properties pros, int numberOfReceivers, StorageLevel storageLevel) {
      JavaStreamingContext jsc = new JavaStreamingContext(ssc);
      return createStream(jsc, pros, numberOfReceivers, storageLevel, new IdentityMessageHandler()).dstream();
    }

    public static <E> JavaDStream<MessageAndMetadata<E>> launch(
            JavaStreamingContext jsc, Properties pros, int numberOfReceivers,
            StorageLevel storageLevel, KafkaMessageHandler<E> messageHandler) {
        return createStream(jsc, pros, numberOfReceivers, storageLevel, messageHandler);
    }

    public static JavaDStream<MessageAndMetadata<byte[]>> launch(
            JavaStreamingContext jsc, Properties pros, int numberOfReceivers, StorageLevel storageLevel) {
        return createStream(jsc, pros, numberOfReceivers, storageLevel, new IdentityMessageHandler());
    }

    private static <E> JavaDStream<MessageAndMetadata<E>> createStream(
            JavaStreamingContext jsc, Properties pros, int numberOfReceivers, StorageLevel storageLevel,
            KafkaMessageHandler<E> messageHandler) {

        AtomicBoolean terminateOnFailure = new AtomicBoolean(false);
        List<JavaDStream<MessageAndMetadata<E>>> streamsList =
                new ArrayList<>();
        JavaDStream<MessageAndMetadata<E>> unionStreams;
        int numberOfPartition;
        KafkaConfig kafkaConfig = new KafkaConfig(pros);
        ZkState zkState = new ZkState(kafkaConfig);
        String numberOfPartitionStr =
                (String) pros.getProperty(Config.KAFKA_PARTITIONS_NUMBER);
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
                streamsList.add(jsc.receiverStream(new KafkaReceiver(
                        pros, i, storageLevel, messageHandler)));
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
                streamsList.add(jsc.receiverStream(new KafkaRangeReceiver(pros, rMap
                        .get(i), storageLevel, messageHandler)));
            }
        }

        // Union all the streams if there is more than 1 stream
        if (streamsList.size() > 1) {
            unionStreams =
                    jsc.union(
                            streamsList.get(0), streamsList.subList(1, streamsList.size()));
        } else {
            // Otherwise, just use the 1 stream
            unionStreams = streamsList.get(0);
        }

        final long batchDuration = jsc.ssc().graph().batchDuration().milliseconds();
        ReceiverStreamListener listener = new ReceiverStreamListener(kafkaConfig,
            batchDuration, numberOfPartition, terminateOnFailure);

        jsc.addStreamingListener(listener);
        //Reset the fetch size
        Utils.setFetchRate(kafkaConfig, kafkaConfig._fetchSizeBytes);
        zkState.close();
        return unionStreams;
    }

    private static int getNumPartitions(ZkState zkState, String topic) {
        try {
            String topicBrokersPath = partitionPath(topic);
            List<String> children =
                    zkState.getCurator().getChildren().forPath(topicBrokersPath);
            return children.size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String partitionPath(String topic) {
        return _zkPath + "/topics/" + topic + "/partitions";
    }
}
