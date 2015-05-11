package consumer.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.util.Utils;

import scala.Function0;
import scala.runtime.BoxedUnit;
import consumer.kafka.client.KafkaRangeReceiver;
import consumer.kafka.client.KafkaReceiver;

public class ReceiverLauncher implements Serializable{
	
	private static final long serialVersionUID = -3008388663855819086L;
	private static String _zkPath;
	private static String _topic;

	
	public static DStream<MessageAndMetadata> launch(StreamingContext ssc , Properties pros, int numberOfReceivers, StorageLevel storageLevel){
		
		JavaStreamingContext jsc = new JavaStreamingContext(ssc);
		return createStream(jsc,pros,numberOfReceivers,storageLevel).dstream();
	}
	
	public static JavaDStream<MessageAndMetadata> launch(JavaStreamingContext jsc , Properties pros, int numberOfReceivers , StorageLevel storageLevel){
	
		return createStream(jsc,pros,numberOfReceivers,storageLevel);
	}
	
	private static JavaDStream<MessageAndMetadata> createStream(JavaStreamingContext jsc, Properties pros, int numberOfReceivers,StorageLevel storageLevel){
		
		List<JavaDStream<MessageAndMetadata>> streamsList = new ArrayList<JavaDStream<MessageAndMetadata>>();
		JavaDStream<MessageAndMetadata> unionStreams;
		int numberOfPartition;
		KafkaConfig kafkaConfig = new KafkaConfig(pros);
		
		if(kafkaConfig._stopGracefully){
			
			//available only in Spark 1.4.0. Commenting it for now .
			
			//addShutdownHook(jsc);
		}
		
		
		String numberOfPartitionStr = (String) pros.getProperty(Config.KAFKA_PARTITIONS_NUMBER);
		if (numberOfPartitionStr != null) {
			numberOfPartition = Integer.parseInt(numberOfPartitionStr);
		} else {
			ZkState zkState = new ZkState(kafkaConfig);
			_zkPath = (String) kafkaConfig._stateConf.get(Config.ZOOKEEPER_BROKER_PATH);
			_topic = (String) kafkaConfig._stateConf.get(Config.KAFKA_TOPIC);
			numberOfPartition = getNumPartitions(zkState);
		}

		//Create as many Receiver as Partition
		if(numberOfReceivers >= numberOfPartition) {
			
			for (int i = 0; i < numberOfPartition; i++) {
				
				streamsList.add(jsc.receiverStream(new KafkaReceiver(pros, i,storageLevel)));

			}
		}else {
			
			//create Range Receivers..
			Map<Integer, Set<Integer>> rMap = new HashMap<Integer, Set<Integer>>();
			
			for (int i = 0; i < numberOfPartition; i++) {

				int j = i % numberOfReceivers ;
				Set<Integer> pSet =   rMap.get(j);
				if(pSet == null) {
					pSet = new HashSet<Integer>();
					pSet.add(i);
				}else {
					pSet.add(i);
				}
				rMap.put(j, pSet);
			}
			
			for (int i = 0; i < numberOfReceivers; i++) {

				streamsList.add(jsc.receiverStream(new KafkaRangeReceiver(pros, rMap.get(i),storageLevel)));
			}
		}


		// Union all the streams if there is more than 1 stream
		if (streamsList.size() > 1) {
			unionStreams = jsc.union(streamsList.get(0),
					streamsList.subList(1, streamsList.size()));
		} else {
			// Otherwise, just use the 1 stream
			unionStreams = streamsList.get(0);
		}
		
		return unionStreams;
	}
	
	private static void addShutdownHook(final JavaStreamingContext jsc) {

		Utils.addShutdownHook(150, new Function0<BoxedUnit>() {
			
			@Override
			public BoxedUnit apply() {
				return null;
			}

			@Override
			public byte apply$mcB$sp() {
				return 0;
			}

			@Override
			public char apply$mcC$sp() {
				return 0;
			}

			@Override
			public double apply$mcD$sp() {
				return 0;
			}

			@Override
			public float apply$mcF$sp() {
				return 0;
			}

			@Override
			public int apply$mcI$sp() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public long apply$mcJ$sp() {
				return 0;
			}

			@Override
			public short apply$mcS$sp() {
				return 0;
			}

			@Override
			public void apply$mcV$sp() {
				
				jsc.stop(false, true);
				
			}

			@Override
			public boolean apply$mcZ$sp() {
				// TODO Auto-generated method stub
				return false;
			}
		});
	}

	private static int getNumPartitions(ZkState zkState) {
		try {
			String topicBrokersPath = partitionPath();
			List<String> children = zkState.getCurator().getChildren().forPath(
					topicBrokersPath);
			return children.size();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static String partitionPath() {
		return _zkPath + "/topics/" + _topic + "/partitions";
	}

}
