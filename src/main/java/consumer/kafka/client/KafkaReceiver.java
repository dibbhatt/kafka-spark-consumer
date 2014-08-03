
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

	public KafkaReceiver(Properties props, int partionCount) {
		super(StorageLevel.MEMORY_ONLY());
		this._props = props;
		_partitionCount = partionCount;
	}

	public void onStart() {
		// Start the thread that receives data over a connection
		new Thread() {
			@Override
			public void run() {
				receive();
			}
		}.start();
	}

	public void onStop() {

	}

	private void receive() {

		try {

			KafkaConfig kafkaConfig = new KafkaConfig(_props);
			ZkState zkState = new ZkState(kafkaConfig);
			List<KafkaConsumer> consumer = new ArrayList<KafkaConsumer>();
			for (int i = 0; i < _partitionCount; i++) {

				KafkaConsumer kConsumer = new KafkaConsumer(kafkaConfig,
						zkState, this);
				kConsumer.open(i);
				consumer.add(kConsumer);
			}

			// Until stopped or connection broken continue reading
			while (!isStopped()) {

				for (KafkaConsumer con : consumer) {

					con.createStream();
				}

			}
			// Restart in an attempt to connect again when server is active
			// again
			restart("Trying to connect again");
		} catch (Throwable t) {
			restart("Error receiving data", t);
		}
	}
}