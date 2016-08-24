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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class ZkBrokerReader implements IBrokerReader, Serializable {

  public static final Logger LOG = LoggerFactory
      .getLogger(ZkBrokerReader.class);

  private GlobalPartitionInformation _cachedBrokers;
  private DynamicBrokersReader _reader;
  private long _lastRefreshTimeMs;
  private long refreshMillis;

  public ZkBrokerReader(KafkaConfig config, ZkState zkState) {
    _reader = new DynamicBrokersReader(config, zkState);
    _cachedBrokers = _reader.getBrokerInfo();
    _lastRefreshTimeMs = System.currentTimeMillis();
    refreshMillis = config._refreshFreqSecs * 1000L;

  }

  @Override
  public GlobalPartitionInformation getCurrentBrokers() {
    long currTime = System.currentTimeMillis();
    if (currTime > _lastRefreshTimeMs + refreshMillis) {
      LOG.info("brokers need refreshing because {} ms have expired", refreshMillis);
      _cachedBrokers = _reader.getBrokerInfo();
      _lastRefreshTimeMs = currTime;
    }
    return _cachedBrokers;
  }

  @Override
  public void close() {
    _reader.close();
  }
}
