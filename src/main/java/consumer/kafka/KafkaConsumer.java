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
import java.util.List;

import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class KafkaConsumer<E extends Serializable> implements Runnable, Serializable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private KafkaConfig _kafkaconfig;
    private PartitionCoordinator _coordinator;
    private DynamicPartitionConnections _connections;
    private ZkState _state;
    private long _lastConsumeTime = 0L;
    private int _currPartitionIndex = 0;
    private Receiver<MessageAndMetadata<E>> _receiver;
    private KafkaMessageHandler _messageHandler;

    public KafkaConsumer(
            KafkaConfig config,
            ZkState zkState,
            Receiver<MessageAndMetadata<E>> receiver,
            KafkaMessageHandler messageHandler) {
        _kafkaconfig = config;
        _state = zkState;
        _receiver = receiver;
        _messageHandler = messageHandler;
    }

    public void open(int partitionId) {
        _currPartitionIndex = partitionId;
        _connections =
                new DynamicPartitionConnections(
                        _kafkaconfig,
                        new ZkBrokerReader(
                                _kafkaconfig,
                                _state));

        _coordinator =
                new ZkCoordinator(
                        _connections,
                        _kafkaconfig,
                        _state,
                        partitionId,
                        _receiver,
                        true,
                        _messageHandler);
    }

    @Override
    public void close() {
      if (_state != null) {
          _state.close();
      }
      if (_connections != null) {
          _connections.clear();
      }
    }

    public void createStream() throws Exception {
      try {
        List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
        if (managers == null || managers.size() == 0) {
            LOG.warn("Some issue getting Partition details.. Refreshing Corodinator..");
            _coordinator.refresh();
        } else {
            managers.get(0).next();
        }
      } catch (FailedFetchException fe) {
          fe.printStackTrace();
          LOG.warn("Fetch failed. Refresing Coordinator..", fe);
          _coordinator.refresh();
      } catch (Exception ex) {
          LOG.error("Partition "
                  + _currPartitionIndex + " encountered error during createStream : "
                  + ex.getMessage());
          ex.printStackTrace();
          throw ex;
      }
    }

    @Override
    public void run() {
      try {
        while (!_receiver.isStopped()) {
          long timeSinceLastPull = System.currentTimeMillis() - _lastConsumeTime;
          if (timeSinceLastPull >= _kafkaconfig._fillFreqMs) {
            _lastConsumeTime = System.currentTimeMillis();
            this.createStream();
          } else {
            long waitTime = _kafkaconfig._fillFreqMs - timeSinceLastPull;
            if (waitTime > 0)
               Thread.sleep(waitTime);
          }
        }
      } catch (Exception ex) {
          try {
              this.close();
              throw ex;
          } catch (Exception e) {
              e.printStackTrace();
          }
      }
    }
}
