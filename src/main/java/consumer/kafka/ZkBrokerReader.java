package consumer.kafka;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBrokerReader implements IBrokerReader, Serializable {

	public static final Logger LOG = LoggerFactory
			.getLogger(ZkBrokerReader.class);

	GlobalPartitionInformation _cachedBrokers;
	DynamicBrokersReader _reader;
	long _lastRefreshTimeMs;

	long refreshMillis;

	public ZkBrokerReader(KafkaConfig config, ZkState zkState) {
		_reader = new DynamicBrokersReader(config, zkState);
		_cachedBrokers = _reader.getBrokerInfo();
		_lastRefreshTimeMs = System.currentTimeMillis();
		refreshMillis = config._refreshFreqSecs * 1000L;

	}

	@Override
	public GlobalPartitionInformation getCurrentBrokers() {
		long currTime = System.currentTimeMillis();
		if (currTime > _lastRefreshTimeMs + refreshMillis) {
			LOG.info("brokers need refreshing because " + refreshMillis
					+ "ms have expired");
			_cachedBrokers = _reader.getBrokerInfo();
			_lastRefreshTimeMs = currTime;
		}
		return _cachedBrokers;
	}

	@Override
	public void close() {
		_reader.close();
	}
}
