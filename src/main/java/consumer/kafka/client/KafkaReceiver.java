
package consumer.kafka.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consumer.kafka.KafkaConfig;
import consumer.kafka.KafkaConsumer;
import consumer.kafka.ZkState;

public class KafkaReceiver extends Receiver<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7978924537344009223L;

	public static final Logger LOG = LoggerFactory
			.getLogger(KafkaReceiver.class);
	private final Properties _props;
	private int _partitionCount;
	private List<KafkaConsumer> _consumer;

	public KafkaReceiver(Properties props, int partionCount) {
		super(StorageLevel.MEMORY_ONLY_SER());
		this._props = props;
		_partitionCount = partionCount;
	}

	public void onStart() {
		// Start the thread that receives data over a connection
		KafkaConfig kafkaConfig = new KafkaConfig(_props);
		ZkState zkState = new ZkState(kafkaConfig);
		_consumer = new ArrayList<KafkaConsumer>();
		
		LOG.info("***Staring Kafka Consumer***");
		
		for (int partitionId = 0; partitionId < _partitionCount; partitionId++) {
			
			KafkaConsumer kConsumer = new KafkaConsumer(kafkaConfig,zkState, this);
			kConsumer.open(partitionId);
			_consumer.add(kConsumer);
			Thread consumerThread = new Thread(kConsumer);
			consumerThread.setDaemon(true);
			consumerThread.start();
		}
	}

	public void onStop() {

		LOG.info("***Stopping Kafka Consumer***");
	}
}