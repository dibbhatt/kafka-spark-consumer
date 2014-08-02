package consumer.kafka;

public interface IBrokerReader {

	GlobalPartitionInformation getCurrentBrokers();

	void close();
}
