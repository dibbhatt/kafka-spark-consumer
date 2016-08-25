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
import java.lang.reflect.Constructor;
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
  public static final Logger LOG = LoggerFactory
      .getLogger(PartitionManager.class);

  private Long _emittedToOffset;
  private Long _lastComittedOffset;
  private Long _lastEnquedOffset;
  private LinkedList<MessageAndOffset> _waitingToEmit =
      new LinkedList<MessageAndOffset>();
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
  private boolean _restart;
  private KafkaMessageHandler _handler;
  private int _numFetchToBuffer = 1;

  @SuppressWarnings("unchecked")
  public PartitionManager(
      DynamicPartitionConnections connections,
        ZkState state,
        KafkaConfig kafkaconfig,
        Partition partiionId,
        Receiver<MessageAndMetadata> receiver,
        boolean restart) {
    _partition = partiionId;
    _connections = connections;
    _kafkaconfig = kafkaconfig;
    _stateConf = _kafkaconfig._stateConf;
    _consumerId = (String) _stateConf.get(Config.KAFKA_CONSUMER_ID);
    _consumer = connections.register(partiionId.host, partiionId.partition);
    _state = state;
    _topic = (String) _stateConf.get(Config.KAFKA_TOPIC);
    _receiver = receiver;
    _restart = restart;
    Constructor<?> constructor;
    try {
      String messageHandlerClass = (String)_stateConf.get(Config.KAFKA_MESSAGE_HANDLER_CLASS);
      constructor = Class.forName(messageHandlerClass).getConstructor();
      _handler = (KafkaMessageHandler) constructor.newInstance();
      LOG.info("Instantiated Message Handler {} for Partition: {}",messageHandlerClass, _partition.getId());
    } catch (Exception e) {
      LOG.error("Not able to instanticate Message Handler Class. Using Identity Handler");
      _handler = new IdentityMessageHandler();
    }

    String consumerJsonId = null;
    Long jsonOffset = null;
    Long processOffset = null;
    String path = committedPath();
    String processPath = processedPath();
    try {
      Map<Object, Object> json = _state.readJSON(path);
      LOG.debug("Read partition information from : {} --> {} ",path, json);
      if (json != null) {
        consumerJsonId = (String)((Map<Object, Object>) json.get("consumer")).get("id");
        jsonOffset = (Long) json.get("offset");
      }
      Map<Object, Object> pJson = _state.readJSON(processPath);
      if (pJson != null) {
        String conId = (String) ((Map<Object, Object>) pJson.get("consumer")).get("id");
        if (conId != null && conId.equalsIgnoreCase(consumerJsonId)) {
          processOffset = (Long) pJson.get("offset");
        }
      }
    } catch (Throwable e) {
      LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
    }
    if (consumerJsonId == null || jsonOffset == null) { // failed to parse JSON?
      _lastComittedOffset = KafkaUtils.getOffset(
          _consumer,
          _topic,
          partiionId.partition,
          kafkaconfig);
      LOG.debug("No partition information found, using configuration to determine offset");
    } else if (!_stateConf.get(Config.KAFKA_CONSUMER_ID).equals(consumerJsonId)
        && kafkaconfig._forceFromStart) {
      _lastComittedOffset = KafkaUtils.getOffset(
          _consumer,
          _topic,
          partiionId.partition,
          kafkaconfig._startOffsetTime);
      LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
    } else {
      if (_restart && processOffset != null && processOffset < jsonOffset) {
        _lastComittedOffset = processOffset + 1;
      } else {
        _lastComittedOffset = jsonOffset;
      }
      LOG.debug("Read last commit offset from zookeeper:{} ; old topology_id:{} - new consumer_id: {}" ,
          _lastComittedOffset,
          consumerJsonId,
          _consumerId);
    }
    LOG.debug("Starting Consumer {} :{} from offset {}",_consumer.host(),partiionId.partition,_lastComittedOffset);
    _emittedToOffset = _lastComittedOffset;
    _lastEnquedOffset = _lastComittedOffset;
  }

  public void next() throws Exception {
    if (_waitingToEmit.isEmpty()) {
      fill();
    } else {
      _numFetchToBuffer = _numFetchToBuffer + 1;
    }

    while (true) {
      MessageAndOffset msgAndOffset = _waitingToEmit.pollFirst();
      if (msgAndOffset != null) {
        Long key = msgAndOffset.offset();
        Message msg = msgAndOffset.message();
        try {
          _lastEnquedOffset = key;
          if (_lastEnquedOffset >= _lastComittedOffset) {
            if (msg.payload() != null) {
              MessageAndMetadata mmeta = new MessageAndMetadata();
              mmeta.setTopic(_topic);
              mmeta.setConsumer(_consumerId);
              mmeta.setOffset(_lastEnquedOffset);
              mmeta.setPartition(_partition);
              byte[] payload = new byte[msg.payload().remaining()];
              msg.payload().get(payload);
              mmeta.setPayload(payload);
              mmeta = _handler.process(mmeta);
              if(mmeta != null) {
                if (msg.hasKey()) {
                  byte[] msgKey = new byte[msg.key().remaining()];
                  msg.key().get(msgKey);
                  mmeta.setKey(msgKey);
                }
                _arrayBuffer.add(mmeta);
                LOG.debug("Store for topic {} for partition {} is {} ",_topic,_partition.partition,_lastEnquedOffset);
              }
            }
          }
        } catch (Exception e) {
          LOG.error("Error Handling MessageAndMetadata",e);
        }
      } else {
        break;
      }
    }
    triggerBlockManagerWrite();
  }

  private void triggerBlockManagerWrite() {
    if(_numFetchToBuffer < _kafkaconfig._numFetchToBuffer) {
      return;
    }
    LOG.debug("Trigger Block Manager Write for Partition {}", _partition.partition);
    if ((_lastEnquedOffset >= _lastComittedOffset)
      && (_waitingToEmit.isEmpty())) {
      try {
        synchronized (_receiver) {
          if (!_arrayBuffer.isEmpty() && !_receiver.isStopped()) {
            _receiver.store(_arrayBuffer.iterator());
            _arrayBuffer.clear();
          }
          commit();
          _numFetchToBuffer = 1;
        }
      } catch (Exception ex) {
        _emittedToOffset = _lastComittedOffset;
        _arrayBuffer.clear();
        if (ex instanceof InterruptedException) {
          throw ex;
        } else {
          _receiver.reportError("Retry Store for Partition " + _partition, ex);
        }
      }
    }
  }

  private void fill() {
    try {
      FetchResponse fetchResponse = KafkaUtils.fetchMessages(
          _kafkaconfig,
          _consumer,
          _partition,
          _emittedToOffset);
      String topic = (String)_stateConf.get(Config.KAFKA_TOPIC);

      if (fetchResponse.hasError()) {
        KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic,_partition.partition));
        if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE)
            && _kafkaconfig._useStartOffsetTimeIfOffsetOutOfRange) {
          long startOffset = KafkaUtils.getOffset(
              _consumer,
              topic,
              _partition.partition,
              _kafkaconfig._startOffsetTime);
          LOG.warn("Got fetch request with offset out of range: {} for Topic {} ."
                    + "retrying with default start offset time from configuration."
                    + "configured latest offset time: {} offset: {}",
                    _emittedToOffset,topic,_kafkaconfig._startOffsetTime,startOffset);
          _emittedToOffset = startOffset;
          LOG.warn("Retyring to fetch again from offset for topic {} from offset {}" ,topic,_emittedToOffset);
          fetchResponse = KafkaUtils.fetchMessages(
              _kafkaconfig,
              _consumer,
              _partition,
              _emittedToOffset);
        } else {
          String message = "Error fetching data from "
              + "["+ _partition.partition+ "] for topic "
              + "["+ topic + "]: ["+ error+ "]";
          LOG.error(message);
          throw new FailedFetchException(message);
        }
      }
      ByteBufferMessageSet msgs = fetchResponse.messageSet(topic, _partition.partition);

      for (MessageAndOffset msg : msgs) {
        if (msg.message() != null) {
          _waitingToEmit.add(msg);
          _emittedToOffset = msg.nextOffset();
        }
      }

      if (_waitingToEmit.size() >= 1)
        LOG.debug("Total {} messages from Kafka: {} : {} there in internal buffers",
            _waitingToEmit.size(),_consumer.host(),_partition.partition);
    } catch (FailedFetchException fe) {
      LOG.error("Exception during fill " + fe.getMessage());
      throw fe;
    }
  }

  public void commit() {
    LOG.debug("LastComitted Offset {}", _lastComittedOffset);
    LOG.debug("New Emitted Offset {}", _emittedToOffset);
    LOG.debug("Enqueued Offset {}", _lastEnquedOffset);

    if (_lastEnquedOffset > _lastComittedOffset) {
      LOG.debug("Committing offset for {}",_partition);
      Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
        .put("consumer",ImmutableMap.of("id", _consumerId))
        .put("offset",_emittedToOffset)
        .put("partition", _partition.partition)
        .put("broker",ImmutableMap.of("host",_partition.host.host,"port",_partition.host.port))
        .put("topic", _topic).build();
      try {
        _state.writeJSON(committedPath(), data);
        LOG.debug("Wrote committed offset to ZK: " + _emittedToOffset);
        _waitingToEmit.clear();
        _lastComittedOffset = _emittedToOffset;
      } catch (Exception zkEx) {
        LOG.error("Error during commit. Let wait for refresh ", zkEx);
      }

      LOG.debug("Committed offset {} for {} for consumer: {}",
          _lastComittedOffset,_partition,_consumerId);
    } else {
      LOG.debug("Last Enqueued offset {} not incremented since previous Comitted Offset {}"
          + " for partition  {} for Consumer {}.",
          _lastEnquedOffset,_lastComittedOffset,_partition, _consumerId);
    }
  }

  private String committedPath() {
    return _stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH) 
        + "/" + _stateConf.get(Config.KAFKA_CONSUMER_ID) 
        + "/" + _stateConf.get(Config.KAFKA_TOPIC)
        + "/"+ _partition.getId();
  }

  public String processedPath() {
    return _stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH)
        + "/"+ _stateConf.get(Config.KAFKA_CONSUMER_ID)
        + "/" + _stateConf.get(Config.KAFKA_TOPIC)
        + "/processed/" + _partition.getId();
  }

  public long queryPartitionOffsetLatestTime() {
    return KafkaUtils.getOffset(_consumer,_topic,_partition.partition,OffsetRequest.LatestTime());
  }

  public long lastCommittedOffset() {
    return _lastComittedOffset;
  }

  public Partition getPartition() {
    return _partition;
  }

  public void close() {
    try {
      LOG.info("Flush BlockManager Write for Partition {} ", _partition.partition);
      _numFetchToBuffer = _kafkaconfig._numFetchToBuffer;
      triggerBlockManagerWrite();
      _connections.unregister(_partition.host, _partition.partition);
      _connections.clear();
      LOG.info("Closed connection for partition : {}", _partition);
    } catch (Exception ex) {
      LOG.error("Error closing connection", ex);
    }

  }
}
