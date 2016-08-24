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

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.curator.test.TestingServer;

public class ZKServerTest {
  private DynamicBrokersReader _dynamicBrokersReader;
  private CuratorFramework _zookeeper;
  private TestingServer _server;
  private SimpleConsumer _simpleConsumer;
  KafkaConfig _config;
  ZkState _zkState;
  private final String _topic = "testtopic";

  @Before
  public void setUp() {

    try {
      _server = new TestingServer();
      String connectionString = _server.getConnectString();
      String[] zkCon = connectionString.split(":");
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
      _zookeeper = CuratorFrameworkFactory.newClient(connectionString,
          new RetryNTimes(5, 1000));
      _dynamicBrokersReader = new DynamicBrokersReader(_config, _zkState);
      _zookeeper.start();

    } catch (Exception e) {

    }

  }

  @After
  public void tearDown() {
    try {
      _server.stop();
      _server.close();
    } catch (Exception e) {

    }
  }

  /*
   * These are Zookeeper Related Test
   */

  private void addPartition(int id, String host, int port) throws Exception {
    writePartitionId(id);
    writeLeader(id, 0);
    writeLeaderDetails(0, host, port);
  }

  private void addPartition(int id, int leader, String host, int port)
      throws Exception {
    writePartitionId(id);
    writeLeader(id, leader);
    writeLeaderDetails(leader, host, port);
  }

  private void writePartitionId(int id) throws Exception {
    String path = _dynamicBrokersReader.partitionPath();
    writeDataToPath(path, ("" + id));
  }

  private void writeDataToPath(String path, String data) throws Exception {
    ZKPaths.mkdirs(_zookeeper.getZookeeperClient().getZooKeeper(), path);
    _zookeeper.setData().forPath(path, data.getBytes());
  }

  private void writeLeader(int id, int leaderId) throws Exception {
    String path = _dynamicBrokersReader.partitionPath() + "/" + id
        + "/state";
    String value = " { \"controller_epoch\":4, \"isr\":[ 1, 0 ], \"leader\":"
        + leaderId + ", \"leader_epoch\":1, \"version\":1 }";
    writeDataToPath(path, value);
  }

  private void writeLeaderDetails(int leaderId, String host, int port)
      throws Exception {
    String path = _dynamicBrokersReader.brokerPath() + "/" + leaderId;
    String value = "{ \"host\":\"" + host
        + "\", \"jmx_port\":9999, \"port\":" + port
        + ", \"version\":1 }";
    writeDataToPath(path, value);
  }

  @Test
  public void testGetBrokerInfo() throws Exception {
    String host = "localhost";
    int port = 9092;
    int partition = 0;
    addPartition(partition, host, port);
    GlobalPartitionInformation brokerInfo = _dynamicBrokersReader
        .getBrokerInfo();
    assertEquals(1, brokerInfo.getOrderedPartitions().size());
    assertEquals(port, brokerInfo.getBrokerFor(partition).port);
    assertEquals(host, brokerInfo.getBrokerFor(partition).host);
  }

  @Test
  public void testMultiplePartitionsOnDifferentHosts() throws Exception {
    String host = "localhost";
    int port = 9092;
    int secondPort = 9093;
    int partition = 0;
    int secondPartition = partition + 1;
    addPartition(partition, 0, host, port);
    addPartition(secondPartition, 1, host, secondPort);

    GlobalPartitionInformation brokerInfo = _dynamicBrokersReader
        .getBrokerInfo();
    assertEquals(2, brokerInfo.getOrderedPartitions().size());

    assertEquals(port, brokerInfo.getBrokerFor(partition).port);
    assertEquals(host, brokerInfo.getBrokerFor(partition).host);

    assertEquals(secondPort, brokerInfo.getBrokerFor(secondPartition).port);
    assertEquals(host, brokerInfo.getBrokerFor(secondPartition).host);
  }

  @Test
  public void testMultiplePartitionsOnSameHost() throws Exception {
    String host = "localhost";
    int port = 9092;
    int partition = 0;
    int secondPartition = partition + 1;
    addPartition(partition, 0, host, port);
    addPartition(secondPartition, 0, host, port);

    GlobalPartitionInformation brokerInfo = _dynamicBrokersReader
        .getBrokerInfo();
    assertEquals(2, brokerInfo.getOrderedPartitions().size());

    assertEquals(port, brokerInfo.getBrokerFor(partition).port);
    assertEquals(host, brokerInfo.getBrokerFor(partition).host);

    assertEquals(port, brokerInfo.getBrokerFor(secondPartition).port);
    assertEquals(host, brokerInfo.getBrokerFor(secondPartition).host);
  }

  @Test
  public void testSwitchHostForPartition() throws Exception {
    String host = "localhost";
    int port = 9092;
    int partition = 0;
    addPartition(partition, host, port);
    GlobalPartitionInformation brokerInfo = _dynamicBrokersReader
        .getBrokerInfo();
    assertEquals(port, brokerInfo.getBrokerFor(partition).port);
    assertEquals(host, brokerInfo.getBrokerFor(partition).host);

    String newHost = host + "switch";
    int newPort = port + 1;
    addPartition(partition, newHost, newPort);
    brokerInfo = _dynamicBrokersReader.getBrokerInfo();
    assertEquals(newPort, brokerInfo.getBrokerFor(partition).port);
    assertEquals(newHost, brokerInfo.getBrokerFor(partition).host);
  }
}
