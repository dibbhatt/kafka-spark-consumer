package consumer.kafka;

import java.io.Serializable;
import java.util.HashMap;

public class Config extends HashMap<String, Object>  implements Serializable{

	/**
	 * Kafka related configurations
	 */
	public static final String ZOOKEEPER_HOSTS = "zookeeper.hosts";
	public static final String ZOOKEEPER_PORT = "zookeeper.port";
	public static final String KAFKA_TOPIC = "kafka.topic";
	public static final String ZOOKEEPER_BROKER_PATH = "zookeeper.broker.path";

	/**
	 * Consumer related configurations
	 */
	public static final String ZOOKEEPER_CONSUMER_PATH = "zookeeper.consumer.path";
	public static final String ZOOKEEPER_CONSUMER_CONNECTION = "zookeeper.consumer.connection";
	public static final String KAFKA_CONSUMER_ID = "kafka.consumer.id";
	public static final String TARGET_INDEXER_CLASS = "target.indexer.class";



}
