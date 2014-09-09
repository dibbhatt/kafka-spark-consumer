package consumer.kafka;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.javaapi.consumer.SimpleConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicPartitionConnections implements Serializable {

	public static final Logger LOG = LoggerFactory
			.getLogger(DynamicPartitionConnections.class);

	class ConnectionInfo implements Serializable {
		SimpleConsumer consumer;
		Set<Integer> partitions = new HashSet();

		public ConnectionInfo(SimpleConsumer consumer) {
			this.consumer = consumer;
		}
	}

	Map<Broker, ConnectionInfo> _connections = new HashMap();
	KafkaConfig _config;
	IBrokerReader _reader;

	public DynamicPartitionConnections(KafkaConfig config,
			IBrokerReader brokerReader) {
		_config = config;
		_reader = brokerReader;
	}

	public SimpleConsumer register(Partition partition) {
		Broker broker = _reader.getCurrentBrokers().getBrokerFor(
				partition.partition);
		return register(broker, partition.partition);
	}

	public SimpleConsumer register(Broker host, int partition) {
		if (!_connections.containsKey(host)) {
			_connections.put(
					host,
					new ConnectionInfo(new SimpleConsumer(host.host, host.port,
							_config._socketTimeoutMs, _config._bufferSizeBytes,
							(String) _config._stateConf
									.get(Config.KAFKA_CONSUMER_ID))));
		}
		ConnectionInfo info = _connections.get(host);
		info.partitions.add(partition);
		return info.consumer;
	}

	public SimpleConsumer getConnection(Partition partition) {
		ConnectionInfo info = _connections.get(partition.host);
		if (info != null) {
			return info.consumer;
		}
		return null;
	}

	public void unregister(Broker port, int partition) {
		ConnectionInfo info = _connections.get(port);
		info.partitions.remove(partition);
		if (info.partitions.isEmpty()) {
			info.consumer.close();
			_connections.remove(port);
		}
	}

	public void unregister(Partition partition) {
		unregister(partition.host, partition.partition);
	}

	public void clear() {
		for (ConnectionInfo info : _connections.values()) {
			info.consumer.close();
		}
		_connections.clear();
	}
}
