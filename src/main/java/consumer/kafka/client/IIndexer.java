package consumer.kafka.client;


public interface IIndexer {

	public void process(byte[] payload) throws Exception;
}
