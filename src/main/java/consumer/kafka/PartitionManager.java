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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import consumer.kafka.client.KafkaReceiver;

public class PartitionManager implements Serializable {
	public static final Logger LOG = LoggerFactory
			.getLogger(PartitionManager.class);

	Long _emittedToOffset;
	Long _lastComittedOffset;
	Long _lastEnquedOffset;
	LinkedList<MessageAndOffset> _waitingToEmit = new LinkedList<MessageAndOffset>();
	Partition _partition;
	KafkaConfig _kafkaconfig;
	String _ConsumerId;
	transient SimpleConsumer _consumer;
	DynamicPartitionConnections _connections;
	ZkState _state;
	String _topic;
	Map _stateConf;
	Long _lastCommitMs = 0l;
	KafkaReceiver _receiver;

	public PartitionManager(DynamicPartitionConnections connections,
			ZkState state, KafkaConfig kafkaconfig, Partition partiionId,
			KafkaReceiver receiver) {
		_partition = partiionId;
		_connections = connections;
		_kafkaconfig = kafkaconfig;
		_stateConf = _kafkaconfig._stateConf;
		_ConsumerId = (String) _stateConf.get(Config.KAFKA_CONSUMER_ID);
		_consumer = connections.register(partiionId.host, partiionId.partition);
		_state = state;
		_topic = (String) _stateConf.get(Config.KAFKA_TOPIC);
		_receiver = receiver;


		String consumerJsonId = null;
		Long jsonOffset = null;
		String path = committedPath();
		try {
			Map<Object, Object> json = _state.readJSON(path);
			LOG.info("Read partition information from: " + path + "  --> "
					+ json);
			if (json != null) {
				consumerJsonId = (String) ((Map<Object, Object>) json
						.get("consumer")).get("id");
				jsonOffset = (Long) json.get("offset");
			}
		} catch (Throwable e) {
			LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
		}

		if (consumerJsonId == null || jsonOffset == null) { // failed to
															// parse JSON?
			_lastComittedOffset = KafkaUtils.getOffset(_consumer, _topic,
					partiionId.partition, kafkaconfig);
			LOG.info("No partition information found, using configuration to determine offset");
		} else if (!_stateConf.get(Config.KAFKA_CONSUMER_ID).equals(consumerJsonId)
				&& kafkaconfig._forceFromStart) {
			_lastComittedOffset = KafkaUtils.getOffset(_consumer, _topic,
					partiionId.partition, kafkaconfig._startOffsetTime);
			LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
		} else {
			_lastComittedOffset = jsonOffset;
			LOG.info("Read last commit offset from zookeeper: "
					+ _lastComittedOffset + "; old topology_id: "
					+ consumerJsonId + " - new consumer_id: " + _ConsumerId);
		}

		LOG.info("Starting Kafka " + _consumer.host() + ":"
				+ partiionId.partition + " from offset " + _lastComittedOffset);
		_emittedToOffset = _lastComittedOffset;
		_lastEnquedOffset = _lastComittedOffset;

	}

	public void next() {
		if (_waitingToEmit.isEmpty()) {

			fill();
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
														
							_receiver.store(msg.payload());
								 
							 LOG.info("Store for topic " + _topic + " for partition " + _partition.partition + " is : "+  _lastEnquedOffset);

						}
					}
				} catch (Exception e) {
					LOG.info("Process Failed for offset " + key + " for  "
							+ _partition + " for topic " + _topic
							+ " with Exception" + e.getMessage());
					e.printStackTrace();
				}
			}else{
				
				break;
			}
		}

		long now = System.currentTimeMillis();
		if ((_lastEnquedOffset > _lastComittedOffset) && ((now - _lastCommitMs) > _kafkaconfig._stateUpdateIntervalMs)) {
			commit();
			LOG.info("After commit , Waiting To Emit queue size is  "
					+ _waitingToEmit.size());
			_lastCommitMs = System.currentTimeMillis();
		}
	}

	private void fill() {

		try {
			FetchResponse fetchResponse = KafkaUtils.fetchMessages(
					_kafkaconfig, _consumer, _partition, _emittedToOffset);

			String topic = (String) _kafkaconfig._stateConf
					.get(Config.KAFKA_TOPIC);

			if (fetchResponse.hasError()) {
				KafkaError error = KafkaError.getError(fetchResponse.errorCode(
						topic, _partition.partition));
				if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE)
						&& _kafkaconfig._useStartOffsetTimeIfOffsetOutOfRange) {
					long startOffset = KafkaUtils
							.getOffset(_consumer, topic, _partition.partition,
									_kafkaconfig._startOffsetTime);
					LOG.warn("Got fetch request with offset out of range: ["
							+ _emittedToOffset
							+ "]; for Topic"
							+ topic
							+ " ."
							+ "retrying with default start offset time from configuration. "
							+ "configured latest offset time: ["
							+ _kafkaconfig._startOffsetTime + "] offset: ["
							+ startOffset + "]");
					_emittedToOffset = startOffset;

					LOG.warn("Retyring to fetch again from offset for topic "
							+ topic + " from offset " + _emittedToOffset);

					fetchResponse = KafkaUtils.fetchMessages(_kafkaconfig,
							_consumer, _partition, _emittedToOffset);

				} else {
					String message = "Error fetching data from ["
							+ _partition.partition + "] for topic [" + topic
							+ "]: [" + error + "]";
					LOG.error(message);
					throw new FailedFetchException(message);
				}
			}

			ByteBufferMessageSet msgs = fetchResponse.messageSet(topic,
					_partition.partition);

			for (MessageAndOffset msg : msgs) {
				if (msg.message() != null) {
					_waitingToEmit.add(msg);
					_emittedToOffset = msg.nextOffset();
				}
			}

			if (_waitingToEmit.size() >= 1)
				LOG.info("Total " + _waitingToEmit.size()
						+ " messages from Kafka: " + _consumer.host() + ":"
						+ _partition.partition + " there in internal buffers");
		} catch (Exception kafkaEx) {
			LOG.error("Exception during fill " + kafkaEx.getMessage());
			kafkaEx.printStackTrace();
		}
	}

	public void commit() {

		LOG.info("LastComitted Offset : " + _lastComittedOffset);
		LOG.info("New Emitted Offset : " + _emittedToOffset);
		LOG.info("Enqueued Offset :" + _lastEnquedOffset);

		if (_lastEnquedOffset > _lastComittedOffset) {
			LOG.info("Committing offset for " + _partition);
			Map<Object, Object> data = (Map<Object, Object>) ImmutableMap
					.builder()
					.put("consumer",
							ImmutableMap.of("id", _ConsumerId))
					.put("offset", _lastEnquedOffset)
					.put("partition", _partition.partition)
					.put("broker",
							ImmutableMap.of("host", _partition.host.host,
									"port", _partition.host.port))
					.put("topic", _topic).build();

			try {
				_state.writeJSON(committedPath(), data);
				LOG.info("Wrote committed offset to ZK: " + _lastEnquedOffset);
				_waitingToEmit.clear();
				_lastComittedOffset = _lastEnquedOffset;
			} catch (Exception zkEx) {
				LOG.error("Error during commit. Let wait for refresh "
						+ zkEx.getMessage());
			}

			LOG.info("Committed offset " + _lastComittedOffset + " for "
					+ _partition + " for consumer: " + _ConsumerId);
			// _emittedToOffset = _lastEnquedOffset;
		} else {

			LOG.info("Last Enqueued offset " + _lastEnquedOffset
					+ " not incremented since previous Comitted Offset "
					+ _lastComittedOffset + " for partition  " + _partition
					+ " for Consumer " + _ConsumerId
					+ ". Some issue in Process!!");
		}
	}

	private String committedPath() {
		return _stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH) + "/"
				+ _stateConf.get(Config.KAFKA_CONSUMER_ID) + "/"
				+ _stateConf.get(Config.KAFKA_TOPIC) + "/" + _partition.getId();
	}

	public long queryPartitionOffsetLatestTime() {
		return KafkaUtils.getOffset(_consumer, _topic, _partition.partition,
				OffsetRequest.LatestTime());
	}

	public long lastCommittedOffset() {
		return _lastComittedOffset;
	}

	public Partition getPartition() {
		return _partition;
	}

	public void close() {
		try {

			_connections.unregister(_partition.host, _partition.partition);
			LOG.info("Closed connection" + " for " + _partition);

		} catch (Exception ex) {

			ex.printStackTrace();
			LOG.error("Error closing connection" + " for " + _partition);
		}

	}
}
