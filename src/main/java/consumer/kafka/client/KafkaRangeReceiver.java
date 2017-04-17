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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consumer.kafka.KafkaConfig;
import consumer.kafka.KafkaConsumer;
import consumer.kafka.KafkaMessageHandler;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ZkState;

@SuppressWarnings("serial")
public class KafkaRangeReceiver<E extends Serializable> extends Receiver<MessageAndMetadata<E>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRangeReceiver.class);
    private final Properties _props;
    private Set<Integer> _partitionSet;
    private KafkaConsumer _kConsumer;
    private transient Thread _consumerThread;
    private List<Thread> _threadList = new ArrayList<Thread>();
    private int maxRestartAttempts;
    private int restartAttempts;
    private KafkaMessageHandler<E> _messageHandler;

    public KafkaRangeReceiver(Properties props,
                              Set<Integer> partitionSet,
                              KafkaMessageHandler messageHandler) {
        this(props, partitionSet, StorageLevel.MEMORY_ONLY(), messageHandler);
    }

    public KafkaRangeReceiver(
            Properties props,
            Set<Integer> partitionSet,
            StorageLevel storageLevel,
            KafkaMessageHandler<E> messageHandler) {
      super(storageLevel);
      this._props = props;
      _partitionSet = partitionSet;
      _messageHandler = messageHandler;
    }

    @Override
    public void onStart() {
      try {
          start();
      } catch (CloneNotSupportedException e) {
          LOG.error("Error while starting Receiver", e);
      }
    }

    public void start() throws CloneNotSupportedException {
        // Start the thread that receives data over a connection
        _threadList.clear();
        KafkaConfig kafkaConfig = new KafkaConfig(_props);
        _messageHandler.init();
        maxRestartAttempts = kafkaConfig._maxRestartAttempts;
        restartAttempts = restartAttempts + 1;
        for (Integer partitionId : _partitionSet) {
          ZkState zkState = new ZkState(kafkaConfig);
          _kConsumer = new KafkaConsumer(kafkaConfig, zkState, this, (KafkaMessageHandler) _messageHandler.clone());
          _kConsumer.open(partitionId);
          Thread.UncaughtExceptionHandler eh =
              new Thread.UncaughtExceptionHandler() {
                  public void uncaughtException(Thread th, Throwable ex) {
                      LOG.error("Receiver got Uncaught Exception " + ex.getMessage()
                              + " for Partition " + partitionId);
                      if (ex instanceof InterruptedException) {
                          LOG.error("Stopping Receiver for partition " + partitionId);
                          th.interrupt();
                          stop(" Stopping Receiver due to " + ex);
                      } else {
                          if (maxRestartAttempts < 0 || restartAttempts < maxRestartAttempts) {
                              LOG.error("Restarting Receiver in 5 Sec for Partition " +
                                      partitionId + " . restart attempt " + restartAttempts);
                              restart("Restarting Receiver for Partition " + partitionId, ex, 5000);
                          } else {
                              LOG.error("tried maximum configured restart attemps " +
                                      maxRestartAttempts + " shutting down receiver");
                              stop(" Stopping Receiver for partition "
                                      + partitionId + ". Max restart attempt exhausted");
                          }
                      }
                  }
              };
          _consumerThread = new Thread(_kConsumer);
          _consumerThread.setUncaughtExceptionHandler(eh);
          _threadList.add(_consumerThread);
          _consumerThread.start();
        }
    }

    @Override
    public void onStop() {
      for (Thread t : _threadList) {
          if (t.isAlive())
              t.interrupt();
      }
    }
}