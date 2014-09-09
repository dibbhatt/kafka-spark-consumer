package consumer.kafka;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtils {

	public static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
	private static final int NO_OFFSET = -5;

	public static long getOffset(SimpleConsumer consumer, String topic,
			int partition, KafkaConfig config) {
		long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		if (config._forceFromStart) {
			startOffsetTime = config._startOffsetTime;
		}
		return getOffset(consumer, topic, partition, startOffsetTime);
	}

	public static long getOffset(SimpleConsumer consumer, String topic,
			int partition, long startOffsetTime) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				startOffsetTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

		long[] offsets = consumer.getOffsetsBefore(request).offsets(topic,
				partition);
		if (offsets.length > 0) {
			return offsets[0];
		} else {
			return NO_OFFSET;
		}
	}

	public static FetchResponse fetchMessages(KafkaConfig config,
			SimpleConsumer consumer, Partition partition, long offset) {
		ByteBufferMessageSet msgs = null;
		String topic = (String) config._stateConf.get(Config.KAFKA_TOPIC);
		int partitionId = partition.partition;

		// for (int errors = 0; errors < 2 && msgs == null; errors++) {
		FetchRequestBuilder builder = new FetchRequestBuilder();
		FetchRequest fetchRequest = builder
				.addFetch(topic, partitionId, offset, config._fetchSizeBytes)
				.clientId(
						(String) config._stateConf
								.get(Config.KAFKA_CONSUMER_ID)).build();
		FetchResponse fetchResponse;
		try {
			fetchResponse = consumer.fetch(fetchRequest);
		} catch (Exception e) {
			if (e instanceof ConnectException) {
				throw new FailedFetchException(e);
			} else {
				throw new RuntimeException(e);
			}
		}

		return fetchResponse;
		/*
		 * if (fetchResponse.hasError()) { KafkaError error =
		 * KafkaError.getError(fetchResponse.errorCode( topic, partitionId)); if
		 * (error.equals(KafkaError.OFFSET_OUT_OF_RANGE) &&
		 * config._useStartOffsetTimeIfOffsetOutOfRange && errors == 0) { long
		 * startOffset = getOffset(consumer, topic, partitionId,
		 * config._startOffsetTime);
		 * LOG.warn("Got fetch request with offset out of range: [" + offset +
		 * "]; for Topic" + topic + " ." +
		 * "retrying with default start offset time from configuration. " +
		 * "configured latest offset time: [" + config._startOffsetTime +
		 * "] offset: [" + startOffset + "]"); offset = startOffset;
		 * 
		 * LOG.warn("Retyring to fetch again from offset for topic " + topic +
		 * " from offset " + offset); fetchRequest = builder .addFetch(topic,
		 * partitionId, offset, config._fetchSizeBytes) .clientId( (String)
		 * config._stateConf .get(Config.KAFKA_BLUR_CONSUMER_ID)) .build();
		 * 
		 * try { fetchResponse = consumer.fetch(fetchRequest); return
		 * fetchResponse.messageSet(topic, partitionId); } catch (Exception e) {
		 * LOG.warn("Fetch failed again"); if (e instanceof ConnectException) {
		 * e.printStackTrace();; } else { e.printStackTrace(); } }
		 * 
		 * 
		 * } else { String message = "Error fetching data from [" + partition +
		 * "] for topic [" + topic + "]: [" + error + "]"; LOG.error(message);
		 * throw new FailedFetchException(message); } } else { msgs =
		 * fetchResponse.messageSet(topic, partitionId); } }
		 */
	}
}
