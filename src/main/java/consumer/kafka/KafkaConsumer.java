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
 *  Kafka Spark Consumer code is taken from Kafka spout of the Apache Storm project (https://github.com/apache/storm/tree/master/external/storm-kafka), 
 *  which was originally created by wurstmeister (https://github.com/wurstmeister/storm-kafka-0.8-plus)
 *  This file has been modified to work with Spark Streaming.
 */

package consumer.kafka;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consumer.kafka.client.KafkaReceiver;

public class KafkaConsumer implements Runnable, Serializable {

	private static final long serialVersionUID = 1780042755212645597L;

	public static final Logger LOG = LoggerFactory
			.getLogger(KafkaConsumer.class);

	KafkaConfig _kafkablurconfig;
	PartitionCoordinator _coordinator;
	DynamicPartitionConnections _connections;
	ZkState _state;
	long _lastUpdateMs = 0;
	int _currPartitionIndex = 0;
	KafkaReceiver _receiver;

	public KafkaConsumer(KafkaConfig blurConfig, ZkState zkState,
			KafkaReceiver receiver) {
		_kafkablurconfig = blurConfig;
		_state = zkState;
		_receiver = receiver;

	}

	public void open(int partitionId) {

		_currPartitionIndex = partitionId;
		_connections = new DynamicPartitionConnections(_kafkablurconfig,
				new ZkBrokerReader(_kafkablurconfig, _state));
		_coordinator = new ZkCoordinator(_connections, _kafkablurconfig,
				_state, partitionId, _receiver, true);

	}

	public void close() {
		_state.close();
	}

	public void createStream() {
		try {
			List<PartitionManager> managers = _coordinator
					.getMyManagedPartitions();
			managers.get(0).next();
		} catch (Exception ex) {
			LOG.error("Partition " + _currPartitionIndex
					+ " encountered error during createStream : "
					+ ex.getMessage());
			ex.printStackTrace();
			throw new RuntimeException("Partition " + _currPartitionIndex
					+ " encountered error during createStream : "
					+ ex.getMessage());
		}

	}

	public void deactivate() {
		commit();
	}

	private void commit() {
		_lastUpdateMs = System.currentTimeMillis();
		_coordinator.getMyManagedPartitions().get(0).commit();
	}

	@Override
	public void run() {

		try {

			while (!_receiver.isStopped()) {

				this.createStream();
			}

		} catch (Throwable t) {

			LOG.error("Error during Receiver Run " + t.getMessage()
					+ " trying to restart");
			t.printStackTrace();
			_receiver.restart("Trying to connect Receiver for Partition "
					+ _currPartitionIndex);

		}

	}

}
