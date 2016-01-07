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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("serial")
public class KafkaConfig implements Serializable {

  public int _fetchSizeBytes = 512 * 1024;
  public int _fillFreqMs = 200;
  public int _minFetchSizeBytes = 1 * 1024;
  public int _bufferSizeBytes = _fetchSizeBytes;
  public int _refreshFreqSecs = 60;
  public int _socketTimeoutMs = 10000;
  public boolean _forceFromStart = false;
  public boolean _stopGracefully = true;
  public boolean _backpressureEnabled = false;
  public long _startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
  public boolean _useStartOffsetTimeIfOffsetOutOfRange = true;
  public long _stateUpdateIntervalMs = 2000;
  public Map<String, String> _stateConf;

  // Parameters related to Back Pressure

  public double _proportional = 0.75;
  public double _integral = 0.15;
  public double _derivative = 0;

  public KafkaConfig(Properties props) {

    String zkHost = props.getProperty("zookeeper.hosts");
    String zkPort = props.getProperty("zookeeper.port");
    String kafkaTopic = props.getProperty("kafka.topic");
    String brokerZkPath = props.getProperty("zookeeper.broker.path");

    String consumerZkPath = props.getProperty("zookeeper.consumer.path");
    String consumerConnection =
        props.getProperty("zookeeper.consumer.connection");
    String consumerId = props.getProperty("kafka.consumer.id");

    if (props.getProperty("consumer.forcefromstart") != null)
      _forceFromStart =
          Boolean.parseBoolean(props.getProperty("consumer.forcefromstart"));

    if (props.getProperty("consumer.fetchsizebytes") != null) {
      _fetchSizeBytes =
          Integer.parseInt(props.getProperty("consumer.fetchsizebytes"));
      _bufferSizeBytes = _fetchSizeBytes;
    }

    if (props.getProperty("consumer.min.fetchsizebytes") != null)
      _minFetchSizeBytes =
          Integer.parseInt(props.getProperty("consumer.min.fetchsizebytes"));

    if (props.getProperty("consumer.fillfreqms") != null)
      _fillFreqMs = Integer.parseInt(props.getProperty("consumer.fillfreqms"));

    if (props.getProperty("consumer.stopgracefully") != null)
      _stopGracefully =
          Boolean.parseBoolean(props.getProperty("consumer.stopgracefully"));

    if (props.getProperty("consumer.backpressure.enabled") != null)
      _backpressureEnabled =
          Boolean.parseBoolean(props
              .getProperty("consumer.backpressure.enabled"));

    if (props.getProperty("consumer.backpressure.proportional") != null)
      _proportional =
          Double.parseDouble(props
              .getProperty("consumer.backpressure.proportional"));

    if (props.getProperty("consumer.backpressure.integral") != null)
      _integral =
          Double.parseDouble(props
              .getProperty("consumer.backpressure.integral"));

    if (props.getProperty("consumer.backpressure.derivative") != null)
      _derivative =
          Double.parseDouble(props
              .getProperty("consumer.backpressure.derivative"));

    _stateConf = new HashMap<String, String>();
    _stateConf.put(Config.ZOOKEEPER_HOSTS, zkHost);
    _stateConf.put(Config.ZOOKEEPER_PORT, zkPort);
    _stateConf.put(Config.KAFKA_TOPIC, kafkaTopic);
    _stateConf.put(Config.ZOOKEEPER_BROKER_PATH, brokerZkPath);

    _stateConf.put(Config.ZOOKEEPER_CONSUMER_PATH, consumerZkPath);
    _stateConf.put(Config.ZOOKEEPER_CONSUMER_CONNECTION, consumerConnection);
    _stateConf.put(Config.KAFKA_CONSUMER_ID, consumerId);
    _stateConf.put(Config.KAFKA_MESSAGE_HANDLER_CLASS, props.getProperty(
        "kafka.message.handler.class",
          "com.instartlogic.calves.kafka.spark.IdentityMessageHandler"));

  }

}
