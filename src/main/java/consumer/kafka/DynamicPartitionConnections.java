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

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class DynamicPartitionConnections implements Serializable {

  public static final Logger LOG = LoggerFactory
      .getLogger(DynamicPartitionConnections.class);

  class ConnectionInfo implements Serializable {

    KafkaConsumer<byte[], byte[]> consumer;
    public ConnectionInfo(KafkaConsumer<byte[], byte[]> consumer, String topic, int partition) {
      this.consumer = consumer;
      TopicPartition tpartition = new TopicPartition(topic, partition);
      List<TopicPartition> lst = new LinkedList<TopicPartition>();
      lst.add(tpartition);
      consumer.assign(lst);
    }
  }

  Map<Broker, ConnectionInfo> _connections = new HashMap<Broker, ConnectionInfo>();
  KafkaConfig _config;
  IBrokerReader _reader;

  public DynamicPartitionConnections(
      KafkaConfig config,
      IBrokerReader brokerReader) {
    _config = config;
    _reader = brokerReader;
  }

  public KafkaConsumer<byte[], byte[]> register(Partition partition, String topic) {
    Broker broker = _reader.getCurrentBrokers().getBrokerFor(partition.partition);
    return register(broker, partition.partition, topic);
  }


  public KafkaConsumer<byte[], byte[]> register(Broker host, int partition, String topic) {
    if (!_connections.containsKey(host)) {
      _connections.put(host, new ConnectionInfo (new KafkaConsumer<byte[], byte[]>(_config.getProperties()), topic, partition));
    }
    ConnectionInfo info = _connections.get(host);
    return info.consumer;
  }

  public KafkaConsumer<byte[], byte[]> getConnection(Partition partition) {
    ConnectionInfo info = _connections.get(partition.host);
    if (info != null) {
      return info.consumer;
    }
    return null;
  }

  public void unregister(Broker host, int partition) {
    ConnectionInfo info = _connections.get(host);
    info.consumer.close();
    _connections.remove(host);
  }

  public void unregister(Partition partition) {
    unregister(partition.host, partition.partition);
  }

  public void clear() {
    for (ConnectionInfo info : _connections.values()) {
      info.consumer.close();
    }
    _connections.clear();
  }
}
