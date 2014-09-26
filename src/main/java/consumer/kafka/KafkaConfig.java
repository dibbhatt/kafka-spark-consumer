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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConfig implements Serializable {

	public int _fetchSizeBytes = 512 * 1024;
	public int _socketTimeoutMs = 10000;
	public int _bufferSizeBytes = 512 * 1024;

	public int _refreshFreqSecs = 100;

	public boolean _forceFromStart = true;
	public long _startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
	public boolean _useStartOffsetTimeIfOffsetOutOfRange = true;

	public long _stateUpdateIntervalMs = 2000;
	public Map _stateConf;

	public KafkaConfig(Properties props) {

		String zkHost = props.getProperty("zookeeper.hosts");
		String zkPort = props.getProperty("zookeeper.port");
		String kafkaTopic = props.getProperty("kafka.topic");
		String brokerZkPath = props.getProperty("zookeeper.broker.path");

		String consumerZkPath = props.getProperty("zookeeper.consumer.path");
		String consumerConnection = props
				.getProperty("zookeeper.consumer.connection");
		String consumerId = props.getProperty("kafka.consumer.id");
		String decoderClass = props.getProperty("target.indexer.class");

		_stateConf = new HashMap();
		List<String> zkServers = new ArrayList<String>(Arrays.asList(zkHost
				.split(",")));
		_stateConf.put(Config.ZOOKEEPER_HOSTS, zkServers);
		_stateConf.put(Config.ZOOKEEPER_PORT, zkPort);
		_stateConf.put(Config.KAFKA_TOPIC, kafkaTopic);
		_stateConf.put(Config.ZOOKEEPER_BROKER_PATH, brokerZkPath);

		_stateConf.put(Config.ZOOKEEPER_CONSUMER_PATH, consumerZkPath);
		_stateConf
				.put(Config.ZOOKEEPER_CONSUMER_CONNECTION, consumerConnection);
		_stateConf.put(Config.KAFKA_CONSUMER_ID, consumerId);
		_stateConf.put(Config.TARGET_INDEXER_CLASS, decoderClass);

	}

}
