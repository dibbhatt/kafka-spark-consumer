package consumer.kafka.client;

import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import consumer.kafka.KafkaConfig;
import consumer.kafka.KafkaConsumer;
import consumer.kafka.ZkState;

public class KafkaReceiver extends Receiver {

	private static final long serialVersionUID = -4927707326026616451L;
	private final Properties _props;
	private int _partitionId;
	private transient KafkaConsumer _kConsumer;

	public KafkaReceiver(Properties props, int partitionId) {
		super(StorageLevel.MEMORY_ONLY_SER());
		this._props = props;
		_partitionId = partitionId;
	}

	public void onStart() {
		// Start the thread that receives data over a connection
		KafkaConfig kafkaConfig = new KafkaConfig(_props);
		ZkState zkState = new ZkState(kafkaConfig);
		_kConsumer = new KafkaConsumer(kafkaConfig, zkState, this);
		_kConsumer.open(_partitionId);
		Thread _consumerThread = new Thread(_kConsumer);
		_consumerThread.start();

	}

	public void onStop() {

		_kConsumer.close();

	}
}