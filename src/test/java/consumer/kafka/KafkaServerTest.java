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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaServerTest {
  private CuratorFramework _zookeeper;
  private KafkaTestBroker _broker;
  private SimpleConsumer _simpleConsumer;
  KafkaConfig _config;
  ZkState _zkState;
  private final String _port = "49123";
  private final String _topic = "testtopic";
  private String _indexDir = "./target/tmp";
  private String _message;

  @Before
  public void setUp() {

    try {
      _broker = new KafkaTestBroker(_indexDir);
      String[] zkCon = _broker.getBrokerConnectionString().split(":");
      Properties props = new Properties();
      props.setProperty("zookeeper.broker.path", "/brokers");
      props.setProperty("zookeeper.consumer.path", "kafka-test");
      props.setProperty("zookeeper.hosts", zkCon[0]);
      props.setProperty("zookeeper.port", zkCon[1]);
      props.setProperty("kafka.topic", _topic);
      props.setProperty("kafka.consumer.id", "1234");
      props.setProperty("target.table.name", "testtable");
      props.setProperty("target.indexer.class",
          "consumer.kafka.TestIndexer");

      _config = new KafkaConfig(props);
      _zkState = new ZkState(_config);
      _zookeeper = CuratorFrameworkFactory.newClient(_broker
          .getBrokerConnectionString(), new RetryNTimes(5, 1000));
      _simpleConsumer = new SimpleConsumer("localhost",
          _broker.getPort(), 60000, 1024, "testClient");
      _zookeeper.start();
      _message = createTopicAndSendMessage();
    } catch (Exception e) {

      e.printStackTrace();
    }

  }

  @After
  public void tearDown() {
    try {
      _simpleConsumer.close();
      _broker.shutdown();
    } catch (Exception e) {

    }
  }

  /*
   * These are Kafka Related Test
   */

  private String createTopicAndSendMessage() {
    Properties props = new Properties();
    props.setProperty("metadata.broker.list", "localhost:" + _port);
    props.put("producer.type", "sync");
    ProducerConfig producerConfig = new ProducerConfig(props);
    kafka.javaapi.producer.Producer<byte[], byte[]> producer = new kafka.javaapi.producer.Producer<byte[], byte[]>(
        producerConfig);
    String value = "testvalue";
    KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>(
        _topic, null, value.getBytes());
    producer.send(data);
    producer.close();
    return value;
  }

  @Test
  public void sendMessageAndAssertValueForOffset() {

    Partition part = new Partition(Broker.fromString(_broker
        .getBrokerConnectionString()), 0);
    FetchResponse fetchResponse = KafkaUtils.fetchMessages(_config,
        _simpleConsumer, part, 0, 1024);

    ByteBufferMessageSet msgs = fetchResponse.messageSet(_topic,
        part.partition);

    String message = new String(Utils.toByteArray(msgs.iterator().next()
        .message().payload()));
    assertThat(message, is(equalTo(_message)));
  }
}
