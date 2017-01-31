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

@SuppressWarnings("serial")
public class Config extends HashMap<String, Object> implements Serializable {

  /**
   * Kafka related configurations
   */
  public static final String ZOOKEEPER_HOSTS = 
      "zookeeper.hosts";
  public static final String ZOOKEEPER_PORT = 
      "zookeeper.port";
  public static final String KAFKA_TOPIC = 
      "kafka.topic";
  public static final String ZOOKEEPER_BROKER_PATH = 
      "zookeeper.broker.path";

  /**
   * Consumer related configurations
   */
  public static final String ZOOKEEPER_CONSUMER_PATH =
      "zookeeper.consumer.path";
  public static final String ZOOKEEPER_CONSUMER_CONNECTION =
      "zookeeper.consumer.connection";
  public static final String KAFKA_CONSUMER_ID = "kafka.consumer.id";

  /**
   * Optional Configurations
   */
  public static final String KAFKA_PARTITIONS_NUMBER =
      "kafka.partitions.number";
  public static final String CONSUMER_FORCE_FROM_START =
      "consumer.forcefromstart";
  public static final String CONSUMER_FETCH_SIZE_BYTES =
      "consumer.fetchsizebytes";
  public static final String CONSUMER_FILL_FREQ_MS = 
      "consumer.fillfreqms";
  public static final String CONSUMER_STOP_GRACEFULLY =
      "consumer.stopgracefully";

  /**
   * Configuration Related to Back Pressure
   */

  public static final String CONSUMER_BACKPRESSURE_ENABLED =
      "consumer.backpressure.enabled";
  public static final String CONSUMER_BACKPRESSURE_PROPORTIONAL =
      "consumer.backpressure.proportional";
  public static final String CONSUMER_BACKPRESSURE_INTEGRAL =
      "consumer.backpressure.integral";
  public static final String CONSUMER_BACKPRESSURE_DERIVATIVE =
      "consumer.backpressure.derivative";

  public static final String KAFKA_RECEIVER_NUMBER = 
      "kafka.receiver.number";
  public static final String SPARK_RESOURCE_MANAGER_CLASS =
      "spark.resource.manager.class";

}
