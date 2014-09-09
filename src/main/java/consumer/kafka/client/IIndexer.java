package consumer.kafka.client;

import java.io.Serializable;

public interface IIndexer extends Serializable {

	public void process(byte[] payload) throws Exception;
}
