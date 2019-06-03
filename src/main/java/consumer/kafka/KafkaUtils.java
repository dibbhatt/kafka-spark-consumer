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

/*
 *   This file is based on the source code of the Kafka spout of the Apache Storm project.
 *   (https://github.com/apache/storm/tree/master/external/storm-kafka)
 *   This file has been modified to work with Spark Streaming.
 */

package consumer.kafka;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtils {

  public static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
  public static final int NO_OFFSET = -5;

  public static long getOffset(
    KafkaConsumer<byte[], byte[]> consumer, String topic, int partition, boolean forceFromStart) {
    TopicPartition  topicAndPartition =
      new TopicPartition (topic, partition);
    Map<TopicPartition, Long > offsetMap = null;
    if(forceFromStart) {
      offsetMap = consumer.beginningOffsets(Arrays.asList(topicAndPartition));
    } else {
      offsetMap = consumer.endOffsets(Arrays.asList(topicAndPartition));
    }
 
    if(offsetMap.get(topicAndPartition) != null ) {
      return offsetMap.get(topicAndPartition);
    } else {
      return NO_OFFSET;
    }
  }

  public static ConsumerRecords<byte[], byte[]> fetchMessages(
    KafkaConfig config, KafkaConsumer<byte[], byte[]> consumer, Partition partition,
    long offset) {
    String topic = (String) config._stateConf.get(Config.KAFKA_TOPIC);
    int partitionId = partition.partition;
    TopicPartition  topicAndPartition = new TopicPartition (topic, partitionId);
    consumer.seek(topicAndPartition, offset);
    ConsumerRecords<byte[], byte[]> records;
    try {
      records = consumer.poll(config._fillFreqMs / 2);
    } catch(InvalidOffsetException ex) {
      throw new OutOfRangeException(ex.getMessage());
    } catch (Exception e) {
      if (e instanceof KafkaException || e instanceof ConnectException
        || e instanceof SocketTimeoutException || e instanceof IOException
        || e instanceof UnresolvedAddressException) {

        LOG.warn("Network error when fetching messages:", e);
        throw new FailedFetchException(e);
      } else {
        throw new RuntimeException(e);
      }
    }
    return records;
  }
}
