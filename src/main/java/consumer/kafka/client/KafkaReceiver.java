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
	private KafkaConsumer _kConsumer;
	private transient Thread _consumerThread;

	public KafkaReceiver(Properties props, int partitionId) {
		super(StorageLevel.MEMORY_ONLY_SER_2());
		this._props = props;
		_partitionId = partitionId;
	}

	@Override
	public void onStart() {
		// Start the thread that receives data over a connection
		KafkaConfig kafkaConfig = new KafkaConfig(_props);
		ZkState zkState = new ZkState(kafkaConfig);
		_kConsumer = new KafkaConsumer(kafkaConfig, zkState, this);
		_kConsumer.open(_partitionId);
		_consumerThread = new Thread(_kConsumer);
		_consumerThread.setDaemon(true);
		_consumerThread.start();

	}

	@Override
	public void onStop() {

		_kConsumer.close();
		_consumerThread.interrupt();

	}
}