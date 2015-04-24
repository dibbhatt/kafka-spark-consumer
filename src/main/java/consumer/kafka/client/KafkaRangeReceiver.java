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

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import consumer.kafka.KafkaConfig;
import consumer.kafka.KafkaConsumer;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ZkState;

public class KafkaRangeReceiver extends Receiver<MessageAndMetadata> {

	private static final long serialVersionUID = -4434734456026616121L;
	private final Properties _props;
	private Set<Integer> _partitionSet ;
	private KafkaConsumer _kConsumer;
	private transient Thread _consumerThread;
	private transient ExecutorService _executorService;

	public KafkaRangeReceiver(Properties props, Set<Integer> partitionSet) {
		super(StorageLevel.MEMORY_ONLY());
		this._props = props;
		_partitionSet = partitionSet;
	}

	public KafkaRangeReceiver(Properties props, Set<Integer> partitionSet, StorageLevel storageLevel) {
		super(storageLevel);
		this._props = props;
		_partitionSet = partitionSet;
	}
	
	@Override
	public void onStart() {
		
		start();

	}

	public void start() {
		
		// Start the thread that receives data over a connection
		_executorService = Executors.newFixedThreadPool(_partitionSet.size());

		for(Integer partitionId : _partitionSet){
			
			KafkaConfig kafkaConfig = new KafkaConfig(_props);
			ZkState zkState = new ZkState(kafkaConfig);
			_kConsumer = new KafkaConsumer(kafkaConfig, zkState, this);
			_kConsumer.open(partitionId);
			
			Thread.UncaughtExceptionHandler eh = new Thread.UncaughtExceptionHandler() {
			    public void uncaughtException(Thread th, Throwable ex) {
			    	restart("Restarting Range Receiver " , ex, 5000);
			    }
			};
			
			_consumerThread = new Thread(_kConsumer);
			_consumerThread.setDaemon(true);
			_consumerThread.setUncaughtExceptionHandler(eh);
			_executorService.submit(_consumerThread);
		}		
	}

	@Override
	public void onStop() {

		_executorService.shutdown();
	}
}