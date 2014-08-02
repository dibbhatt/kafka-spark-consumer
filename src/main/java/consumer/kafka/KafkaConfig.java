package consumer.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConfig implements Serializable {

	public int _fetchSizeBytes = 256 * 1024;
	public int _socketTimeoutMs = 10000;
	public int _bufferSizeBytes = 256 * 1024;

	public int _refreshFreqSecs = 100;

	public boolean _forceFromStart = true;
	public long _startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
	public boolean _useStartOffsetTimeIfOffsetOutOfRange = true;

	public long _stateUpdateIntervalMs = 10000;
	public Map _stateConf;

	public KafkaConfig(Properties props) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		
		
		String zkHost = props.getProperty("zookeeper.hosts");
		String zkPort = props.getProperty("zookeeper.port");
		String kafkaTopic = props.getProperty("kafka.topic");
		String brokerZkPath = props.getProperty("zookeeper.broker.path");
		
		String blurConsumerZkPath = props.getProperty("zookeeper.consumer.path");
		String zkBlurConsumerConnection = props.getProperty("zookeeper.consumer.connection");
		String blurConsumerId = props.getProperty("kafka.consumer.id");
		String blurTableName = props.getProperty("target.table.name");
		String blurIndexerClass = props.getProperty("target.indexer.class");

		_stateConf = new HashMap();
		List<String> zkServers = new ArrayList<String>(Arrays.asList(zkHost
				.split(",")));
		_stateConf.put(Config.ZOOKEEPER_HOSTS, zkServers);
		_stateConf.put(Config.ZOOKEEPER_PORT, zkPort);
		_stateConf.put(Config.KAFKA_TOPIC, kafkaTopic);
		_stateConf.put(Config.ZOOKEEPER_BROKER_PATH, brokerZkPath);

		_stateConf.put(Config.ZOOKEEPER_CONSUMER_PATH, blurConsumerZkPath);
		_stateConf.put(Config.ZOOKEEPER_CONSUMER_CONNECTION, zkBlurConsumerConnection);
		_stateConf.put(Config.KAFKA_CONSUMER_ID, blurConsumerId);
		_stateConf.put(Config.TARGET_TABLE_NAME, blurTableName);
		_stateConf.put(Config.TARGET_INDEXER_CLASS, blurIndexerClass);

	}

}
