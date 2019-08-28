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
            JavaStreamingContext jsc, Properties props, int numberOfReceivers, StorageLevel storageLevel,
            KafkaMessageHandler<E> messageHandler) {

        List<JavaDStream<MessageAndMetadata<E>>> streamsList =
                new ArrayList<>();
        JavaDStream<MessageAndMetadata<E>> unionStreams;
        KafkaConfig globalConfig = new KafkaConfig(props);
        _zkPath = (String) globalConfig.brokerZkPath;
        String[] topicList = props.getProperty(Config.KAFKA_TOPIC).split(",");
        int totalPartitions = 0;
        Map<String, KafkaConfig> topicConfigMap = new HashMap<>();

        for(String topic : topicList) {
            Properties property = new Properties();
            property.putAll(props);
            property.replace(Config.KAFKA_TOPIC, topic.trim());
            KafkaConfig kafkaConfig = new KafkaConfig(property);
            ZkState zkState = new ZkState(kafkaConfig);
            int numberOfPartition = getNumPartitions(zkState, topic.trim());
            totalPartitions = totalPartitions + numberOfPartition;
            zkState.close();
            topicConfigMap.put(topic + ":" + numberOfPartition, kafkaConfig);
        }

        for(Map.Entry<String, KafkaConfig> entry : topicConfigMap.entrySet()) {
            String[] tp = entry.getKey().split(":");
            int partitions = Integer.parseInt(tp[1]);
            KafkaConfig config = entry.getValue();
            int assignedReceivers = (int)Math.round(((partitions/(double)totalPartitions) * numberOfReceivers));
            if(assignedReceivers == 0)
                assignedReceivers = 1;

            assignReceiversToPartitions(assignedReceivers,partitions,streamsList, config, storageLevel, messageHandler, jsc);
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
        ReceiverStreamListener listener = new ReceiverStreamListener(globalConfig, batchDuration);
        jsc.addStreamingListener(listener);
        //Reset the fetch size
        Utils.setFetchRate(globalConfig, globalConfig._pollRecords);
        return unionStreams;
    }

    private static <E> void assignReceiversToPartitions(int numberOfReceivers, 
            int numberOfPartition, List<JavaDStream<MessageAndMetadata<E>>> streamsList, 
            KafkaConfig config, StorageLevel storageLevel, KafkaMessageHandler<E> messageHandler, JavaStreamingContext jsc ) {

        // Create as many Receiver as Partition
        if (numberOfReceivers >= numberOfPartition) {
            for (int i = 0; i < numberOfPartition; i++) {
                streamsList.add(jsc.receiverStream(new KafkaReceiver(
                        config, i, storageLevel, messageHandler)));
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
                streamsList.add(jsc.receiverStream(new KafkaRangeReceiver(config, rMap
                        .get(i), storageLevel, messageHandler)));
            }
        }
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
