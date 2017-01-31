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
public class KafkaReceiver<E extends Serializable> extends Receiver<MessageAndMetadata<E>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaReceiver.class);
    private final Properties _props;
    private int _partitionId;
    private KafkaConsumer _kConsumer;
    private transient Thread _consumerThread;
    private int maxRestartAttempts;
    private int restartAttempts;
    private KafkaMessageHandler _messageHandler;

    public KafkaReceiver(Properties props,
                         int partitionId,
                         KafkaMessageHandler messageHandler) {
        this(props, partitionId, StorageLevel.MEMORY_ONLY(), messageHandler);
    }

    public KafkaReceiver(
            Properties props,
            int partitionId,
            StorageLevel storageLevel,
            KafkaMessageHandler messageHandler) {
        super(storageLevel);
        this._props = props;
        this._partitionId = partitionId;
        this._messageHandler = messageHandler;
    }

    @Override
    public void onStart() {
        start();
    }

    public void start() {
        // Start the thread that receives data over a connection
        KafkaConfig kafkaConfig = new KafkaConfig(_props);
        _messageHandler.init();
        maxRestartAttempts = kafkaConfig._maxRestartAttempts;
        restartAttempts = restartAttempts + 1;
        ZkState zkState = new ZkState(kafkaConfig);
        _kConsumer = new KafkaConsumer(kafkaConfig, zkState, this, _messageHandler);
        _kConsumer.open(_partitionId);

        Thread.UncaughtExceptionHandler eh = new Thread.UncaughtExceptionHandler() {

            public void uncaughtException(Thread th, Throwable ex) {

                LOG.error("Receiver got Uncaught Exception " + ex.getMessage()
                        + " for Partition " + _partitionId);
                if (ex instanceof InterruptedException) {
                    LOG.error("Stopping Receiver for partition " + _partitionId);
                    th.interrupt();
                    stop(" Stopping Receiver for partition "
                            + _partitionId + " due to " + ex);

                } else {
                    if (maxRestartAttempts < 0 || restartAttempts < maxRestartAttempts) {
                        LOG.error("Restarting Receiver in 5 Sec for Partition " +
                                _partitionId + " . restart attempt " + restartAttempts);
                        restart("Restarting Receiver for Partition " + _partitionId, ex, 5000);
                    } else {
                        LOG.error("tried maximum configured restart attemps " +
                                maxRestartAttempts + " shutting down receiver");
                        stop(" Stopping Receiver for partition "
                                + _partitionId + ". Max restart attempt exhausted");
                    }
                }
            }
        };
        _consumerThread = new Thread(_kConsumer);
        _consumerThread.setUncaughtExceptionHandler(eh);
        _consumerThread.start();
    }

    @Override
    public void onStop() {
        if (_consumerThread.isAlive()) {
            _consumerThread.interrupt();
        }
    }
}