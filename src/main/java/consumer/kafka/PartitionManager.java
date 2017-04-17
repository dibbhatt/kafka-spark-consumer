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
import java.util.LinkedList;
import java.util.Map;

import kafka.api.OffsetRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

@SuppressWarnings("serial")
public class PartitionManager implements Serializable {
    private static final Logger LOG = LoggerFactory
            .getLogger(PartitionManager.class);
    private Long _emittedToOffset;
    private Long _lastComittedOffset;
    private long _lastEnquedOffset;
    private LinkedList<MessageAndMetadata> _arrayBuffer =
            new LinkedList<MessageAndMetadata>();
    private Partition _partition;
    private KafkaConfig _kafkaconfig;
    private String _consumerId;
    private transient SimpleConsumer _consumer;
    private DynamicPartitionConnections _connections;
    private ZkState _state;
    private String _topic;
    private Map<String, String> _stateConf;
    private Receiver<MessageAndMetadata> _receiver;
    boolean _restart;
    private KafkaMessageHandler _handler;
    private int _numFetchBuffered = 1;

    public PartitionManager(
            DynamicPartitionConnections connections,
            ZkState state,
            KafkaConfig kafkaconfig,
            Partition partitionId,
            Receiver<MessageAndMetadata> receiver,
            boolean restart,
            KafkaMessageHandler messageHandler) {
        _partition = partitionId;
        _connections = connections;
        _kafkaconfig = kafkaconfig;
        _stateConf = _kafkaconfig._stateConf;
        _consumerId = (String) _stateConf.get(Config.KAFKA_CONSUMER_ID);
        _consumer = connections.register(partitionId.host, partitionId.partition);
        _state = state;
        _topic = (String) _stateConf.get(Config.KAFKA_TOPIC);
        _receiver = receiver;
        _restart = restart;
        _handler = messageHandler;

        Long processOffset = null;
        Long consumedOffset = null;
        String processPath = zkPath("processed");
        String consumedPath = zkPath("offsets");

        try {
            byte[] pOffset = _state.readBytes(processPath);
            LOG.info("Read processed information from: {}", processPath);
            if (pOffset != null) {
                processOffset = Long.valueOf(new String(pOffset));
                LOG.info("Processed offset for Partition : {} is {}",_partition.partition, processOffset);
            }
            byte[] conffset = _state.readBytes(consumedPath);
            LOG.info("Read consumed information from: {}", consumedPath);
            if (conffset != null) {
                consumedOffset = Long.valueOf(new String(conffset));
                LOG.info("Consumed offset for Partition : {} is {}",_partition.partition, consumedOffset);
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode", e);
            throw e;
        }
        // failed to parse JSON?
        if (consumedOffset == null) {
            _lastComittedOffset =
                    KafkaUtils.getOffset(
                            _consumer, _topic, _partition.partition, kafkaconfig);
            LOG.info("No partition information found, using configuration to determine offset");
        } else {
          if (_restart && processOffset != null) {
            _lastComittedOffset = processOffset + 1;
        } else {
            _lastComittedOffset = consumedOffset;
        }
    }

      LOG.info("Starting Receiver  {} : {} from offset {}", _consumer.host(), _partition.partition, _lastComittedOffset);
      _emittedToOffset = _lastComittedOffset;
      _lastEnquedOffset = _lastComittedOffset;
      setZkCoordinator();
    }

    //Used for Consumer offset Lag
    private void setZkCoordinator() {
      try{
        String cordinate = String.format("Receiver-%s", _receiver.streamId());
        _state.writeBytes(zkPath("owners"), (cordinate + "-0").getBytes());
        Map<Object, Object> data =
            (Map<Object, Object>) ImmutableMap
              .builder()
              .put("version", 1)
              .put("subscription", ImmutableMap.of(_topic, 1))
              .put("pattern", "static")
              .put("timestamp", Long.toString(System.currentTimeMillis()))
              .build();
        _state.writeJSON(zkIdsPath("ids") + cordinate , data);
      } catch(Exception ne) {
        LOG.error("Node already exists" , ne);
      }
    }

    //Called every Fill Frequency
    public void next() throws Exception {
      fill();
      //If consumer.num_fetch_to_buffer is default (1) , let commit consumed offset after every fill
      //Otherwise consumed offset will be written after buffer is filled during triggerBlockManagerWrite
      if ((_kafkaconfig._numFetchToBuffer == 1) && (_lastEnquedOffset >= _lastComittedOffset)) {
        try {
          _lastComittedOffset = _emittedToOffset;
          _state.writeBytes(zkPath("offsets"), Long.toString(_lastComittedOffset).getBytes());
          LOG.debug("Consumed offset {} for Partition {} written to ZK", _lastComittedOffset, _partition.partition);
        } catch (Exception ex) {
          LOG.error("error during ZK Commit", ex);
          _receiver.reportError("Retry ZK Commit for Partition " + _partition, ex);
        }
      }
    }

    private long getKafkaOffset() {
      long kafkaOffset = KafkaUtils.getOffset(_consumer, _topic, _partition.partition, -1);
      if (kafkaOffset == KafkaUtils.NO_OFFSET) {
          LOG.warn("kafka latest offset not found for partition {}", _partition.partition);
      }
      return kafkaOffset;
    }

    private void reportOffsetLag() {
      try {
          long offsetLag = calculateOffsetLag();
          LOG.info("Offset Lag for Parittion {} is {} " , _partition.partition, offsetLag);
      } catch (Exception e) {
          LOG.error("failed to report offset lag to graphite", e);
      }
    }

    private long calculateOffsetLag() {
      long offsetLag = 0;
      long kafkaOffset = getKafkaOffset();
      if (kafkaOffset != KafkaUtils.NO_OFFSET) {
          offsetLag = kafkaOffset - _lastComittedOffset;
      }
      return offsetLag;
    }


    //This is called when consumer.num_fetch_to_buffer is set and when buffer is filled and 
    //written to Spark Block Manager during fill
    private void triggerBlockManagerWrite() {
      if ((_lastEnquedOffset >= _lastComittedOffset)) {
        try {
          synchronized (_receiver) {
            if (!_arrayBuffer.isEmpty() && !_receiver.isStopped()) {
              _receiver.store(_arrayBuffer.iterator());
              _arrayBuffer.clear();
            }
            _numFetchBuffered = 1;
            _lastComittedOffset = _emittedToOffset;
            //Write consumed offset to ZK
            _state.writeBytes(zkPath("offsets"), Long.toString(_lastComittedOffset).getBytes());
            LOG.debug("Consumed offset {} for Partition {} written to ZK", _lastComittedOffset, _partition.partition);
          }
        } catch (Exception ex) {
          _arrayBuffer.clear();
          LOG.error("error while triggerBlockManagerWrite" , ex);
          _receiver.reportError("Retry Store for Partition " + _partition, ex);
        }
      }
    }

    //Read from Kafka and write to Spark BlockManager
    private void fill() {
      String topic = _kafkaconfig._stateConf.get(Config.KAFKA_TOPIC);
      ByteBufferMessageSet msgs;
      //Get the present fetchSize from ZK set by PID Controller
      int fetchSize = getFetchSize();
      //Fetch messages from Kafka
      msgs = fetchMessages(fetchSize, topic);
      for (MessageAndOffset msgAndOffset : msgs) {
        if (msgAndOffset.message() != null) {
          long key = msgAndOffset.offset();
          Message msg = msgAndOffset.message();
          _emittedToOffset = msgAndOffset.nextOffset();
          _lastEnquedOffset = key;
          //Process only when fetched messages are having higher offset than last committed offset
          if (_lastEnquedOffset >= _lastComittedOffset) {
            if (msg.payload() != null) {
              byte[] payload = new byte[msg.payload().remaining()];
              msg.payload().get(payload);
              MessageAndMetadata<?> mm = null;
              try {
                //Perform Message Handling if configured.
                mm = _handler.handle(_lastEnquedOffset, _partition, _topic, _consumerId,  payload);
                if (msg.hasKey()) {
                  byte[] msgKey = new byte[msg.key().remaining()];
                  msg.key().get(msgKey);
                  mm.setKey(msgKey);
                }
              } catch (Exception e) {
                LOG.error("Process Failed for offset {} partition {} topic {}", key, _partition, _topic, e);
              }
              if (_kafkaconfig._numFetchToBuffer > 1) {
                // Add to buffer
                if(mm != null ) {
                  _arrayBuffer.add(mm);
                  _numFetchBuffered = _numFetchBuffered + 1;
                }
                //Trigger write when buffer reach the limit
                LOG.debug("number of fetch buffered for partition {} is {}", _partition.partition, _numFetchBuffered);
                if (_numFetchBuffered >  _kafkaconfig._numFetchToBuffer) {
                  triggerBlockManagerWrite();
                  LOG.debug("Trigger BM write till offset {} for Partition {}", _lastEnquedOffset, _partition.partition);
                }
              } else {
                //nothing to buffer. Just add to Spark Block Manager
                try {
                  synchronized (_receiver) {
                    if(mm != null) {
                      _receiver.store(mm);
                      LOG.debug("PartitionManager sucessfully written offset {} for partition {} to BM", _lastEnquedOffset, _partition.partition);
                    }
                  }
                } catch (Exception ex) {
                    _receiver.reportError("Retry Store for Partition " + _partition, ex);
                }
              }
            }
          }
        }
      }
    }

    //Invoke Kafka API to fetch messages
    private ByteBufferMessageSet fetchMessages(int fetchSize, String topic) {
      FetchResponse fetchResponse = KafkaUtils.fetchMessages(
          _kafkaconfig, _consumer, _partition, _emittedToOffset, fetchSize);
      if (fetchResponse.hasError()) {
          KafkaError error = KafkaError.getError(fetchResponse.errorCode(
              topic, _partition.partition));
          if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE)) {
             long earliestTime = kafka.api.OffsetRequest.EarliestTime();
             long latestTime = kafka.api.OffsetRequest.LatestTime();
             long earliestOffset = KafkaUtils.getOffset(_consumer, topic, _partition.partition, earliestTime);
             long latestOffset = KafkaUtils.getOffset(_consumer, topic, _partition.partition, latestTime);
             LOG.warn("Got fetch request with offset out of range: {} for Topic {}  partition {}" , _emittedToOffset, topic, _partition.partition);

             //If OFFSET_OUT_OF_RANGE , check if present _emittedToOffset is greater than Partition's latest offset
             //This can happen if new Leader is behind the previous leader when elected during a Kafka broker failure.
             if(_emittedToOffset >= latestOffset) {
               _emittedToOffset = latestOffset;
               LOG.warn("Offset reset to LatestTime {} for Topic {}  partition {}" , _emittedToOffset, topic, _partition.partition);
             } else if (_emittedToOffset <= earliestOffset) {
               //This can happen if messages are deleted from Kafka due to Kafka's log retention period and 
               //probably there is huge lag in Consumer.  Or consumer is stopped for long time.
               _emittedToOffset = earliestOffset;
               _lastComittedOffset = earliestOffset;
               LOG.warn("Offset reset to EarliestTime {} for Topic {}  partition {}" , _emittedToOffset, topic, _partition.partition);
             }
            fetchResponse = KafkaUtils.fetchMessages(
                _kafkaconfig, _consumer, _partition, _emittedToOffset, fetchSize);
          } else {
              String message = "Error fetching data from ["
                              + _partition.partition + "] for topic [" + topic + "]: [" + error + "]";
              LOG.error(message);
              throw new FailedFetchException(message);
          }
      }
      return fetchResponse.messageSet(topic, _partition.partition);
    }

    //Get fetchSize from ZK
    private int getFetchSize() {
      int newFetchSize = 0;
      try {
        byte[] rate = _state.readBytes(ratePath());
        if (rate != null) {
            newFetchSize = Integer.valueOf(new String(rate));
            LOG.debug("Current Fetch Rate for topic {} is {}",
                _kafkaconfig._stateConf.get(Config.KAFKA_TOPIC), newFetchSize);
         }  else {
            newFetchSize = _kafkaconfig._fetchSizeBytes;
        }
      } catch (Throwable e) {
          newFetchSize = _kafkaconfig._fetchSizeBytes;
      }
      return newFetchSize;
  }

    public String ratePath() {
      return _kafkaconfig._stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH)
              + "/" + _kafkaconfig._stateConf.get(Config.KAFKA_CONSUMER_ID) + "/newrate";
    }

    private String zkPath(String type) {
      return _stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH)
              + "/" + _stateConf.get(Config.KAFKA_CONSUMER_ID) + "/" + type+ "/"
              + _stateConf.get(Config.KAFKA_TOPIC) + "/" + _partition.getId();
    }

    private String zkIdsPath(String type) {
      return _stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH)
              + "/" + _stateConf.get(Config.KAFKA_CONSUMER_ID) + "/" + type+ "/"
              + _stateConf.get(Config.KAFKA_TOPIC) + "/";
    }

    public long queryPartitionOffsetLatestTime() {
      return KafkaUtils.getOffset(
              _consumer, _topic, _partition.partition, OffsetRequest.LatestTime());
    }

    public long lastCommittedOffset() {
      return _lastComittedOffset;
    }

    public Partition getPartition() {
      return _partition;
    }

    public void close() {
      try {
        LOG.warn("Flush BlockManager Write for Partition {}", _partition.partition);
        _numFetchBuffered = _kafkaconfig._numFetchToBuffer;
        triggerBlockManagerWrite();
        _connections.unregister(_partition.host, _partition.partition);
        _connections.clear();
        _state.close();
        LOG.info("Closed connection for {}", _partition);
      } catch (Exception ex) {
        ex.printStackTrace();
        LOG.error("Error closing connection" + " for " + _partition);
      }
    }
}
