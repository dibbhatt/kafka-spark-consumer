package consumer.kafka.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.ImmutableMap;

import consumer.kafka.Config;
import consumer.kafka.KafkaConfig;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ZkState;

public class Consumer implements Serializable {

	private static final long serialVersionUID = 4332618245650072140L;
	private Properties _props;
	private KafkaConfig _kafkaConfig;

	public void start() throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		_kafkaConfig = new KafkaConfig(_props);
		run();
	}

	private void init(String[] args) throws Exception {

		Options options = new Options();
		this._props = new Properties();

		options.addOption("p", true, "properties filename from the classpath");
		options.addOption("P", true, "external properties filename");

		OptionBuilder.withArgName("property=value");
		OptionBuilder.hasArgs(2);
		OptionBuilder.withValueSeparator();
		OptionBuilder.withDescription("use value for given property");
		options.addOption(OptionBuilder.create("D"));

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption('p')) {
			this._props.load(ClassLoader.getSystemClassLoader()
					.getResourceAsStream(cmd.getOptionValue('p')));
		}
		if (cmd.hasOption('P')) {
			File file = new File(cmd.getOptionValue('P'));
			FileInputStream fStream = new FileInputStream(file);
			this._props.load(fStream);
		}
		this._props.putAll(cmd.getOptionProperties("D"));

	}

	private void run() {

		String checkpointDirectory = "hdfs://10.252.5.113:9000/user/hadoop/spark";
		int _partitionCount = 3;

		List<JavaDStream<MessageAndMetadata>> streamsList = new ArrayList<JavaDStream<MessageAndMetadata>>(
				_partitionCount);
		JavaDStream<MessageAndMetadata> unionStreams;

		SparkConf _sparkConf = new SparkConf().setAppName("KafkaReceiver").set(
				"spark.streaming.blockInterval", "200");

		JavaStreamingContext ssc = new JavaStreamingContext(_sparkConf,
				new Duration(10000));

		for (int i = 0; i < _partitionCount; i++) {

			streamsList.add(ssc.receiverStream(new KafkaReceiver(_props, i)));

		}

		// Union all the streams if there is more than 1 stream
		if (streamsList.size() > 1) {
			unionStreams = ssc.union(streamsList.get(0),
					streamsList.subList(1, streamsList.size()));
		} else {
			// Otherwise, just use the 1 stream
			unionStreams = streamsList.get(0);
		}

		unionStreams.checkpoint(new Duration(10000));

		try {
			unionStreams
					.foreachRDD(new Function2<JavaRDD<MessageAndMetadata>, Time, Void>() {

						long lastRefreshTime = 0L;
						ZkState zkState = new ZkState(
								(String) _kafkaConfig._stateConf
										.get(Config.ZOOKEEPER_CONSUMER_CONNECTION));
						transient Constructor constructor = Class.forName(
								(String) _kafkaConfig._stateConf
										.get(Config.TARGET_INDEXER_CLASS))
								.getConstructor(String.class);
						transient IIndexer indexer = (IIndexer) constructor
								.newInstance(_props.getProperty("kafka.topic"));

						@Override
						public Void call(JavaRDD<MessageAndMetadata> rdd,
								Time time) throws Exception {

							for (MessageAndMetadata record : rdd.collect()) {

								if (record != null) {

									try {

										indexer.process(record.getPayload());
										
										//Use ack to replay messages when Driver failed. Need to re-look it for a better design.
										//ack(record, zkState);

									} catch (Exception ex) {

										ex.printStackTrace();
									}

								}

							}
							return null;
						}

						public String committedPath() {
							return _kafkaConfig._stateConf
									.get(Config.ZOOKEEPER_CONSUMER_PATH)
									+ "/"
									+ _kafkaConfig._stateConf
											.get(Config.KAFKA_CONSUMER_ID)
									+ "/"
									+ _kafkaConfig._stateConf
											.get(Config.KAFKA_TOPIC)
									+ "/processed/";
						}

						public void ack(MessageAndMetadata record,
								ZkState zkState) {

							if ((System.currentTimeMillis() - lastRefreshTime) > 5000) {

								Map<Object, Object> metadata = (Map<Object, Object>) ImmutableMap
										.builder()
										.put("consumer",
												ImmutableMap.of("id",
														record.getConsumer()))
										.put("offset", record.getOffset())
										.put("partition",
												record.getPartition().partition)
										.put("topic", record.getTopic())
										.build();

								String path = committedPath()
										+ record.getPartition().getId();
								zkState.writeJSON(path, metadata);
								lastRefreshTime = System.currentTimeMillis();
							}

						}
					});
		} catch (Exception ex) {

			ex.printStackTrace();
		}

		ssc.checkpoint(checkpointDirectory);
		ssc.start();
		ssc.awaitTermination();
	}

	public static void main(String[] args) throws Exception {

		Consumer consumer = new Consumer();
		consumer.init(args);
		consumer.start();
	}
}
