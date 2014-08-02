package consumer.kafka.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
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
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consumer.kafka.Config;
import consumer.kafka.DynamicBrokersReader;
import consumer.kafka.KafkaConfig;
import consumer.kafka.ZkState;

public class Consumer implements Serializable {

	public static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
	private IIndexer _indexer;
	private final Properties _props;

	public Consumer() {

		this._props = new Properties();
	}

	private void run(String[] args) throws Exception {

		Options options = new Options();

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

		run();
	}

	private void run() throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		String topic = _props.getProperty("kafka.topic");

		try {

			KafkaConfig kafkaConfig = new KafkaConfig(_props);
			Constructor constructor;
			try {

				constructor = Class.forName(
						(String) kafkaConfig._stateConf
								.get(Config.TARGET_INDEXER_CLASS))
						.getConstructor(String.class);

				_indexer = (IIndexer) constructor.newInstance(topic);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}

			ZkState zkState = new ZkState(kafkaConfig);
			DynamicBrokersReader kafkaBrokerReader = new DynamicBrokersReader(
					kafkaConfig, zkState);
			int partionCount = kafkaBrokerReader.getNumPartitions();

			SparkConf _sparkConf = new SparkConf().setAppName("KafkaReceiver");

			final JavaStreamingContext ssc = new JavaStreamingContext(
					_sparkConf, new Duration(10000));

			final JavaReceiverInputDStream<String> inputStream = ssc
					.receiverStream(new KafkaReceiver(_props, partionCount));
			ssc.checkpoint("/home/spark-1.0.1-bin-hadoop1/work");

			inputStream
					.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
						@Override
						public Void call(JavaRDD<String> rdd, Time time)
								throws Exception {

							for (String record : rdd.collect()) {

								if (record != null) {

									try {

										_indexer.process(record.getBytes());

									} catch (Exception ex) {

										ex.printStackTrace();
										LOG.error("Error During RDD Process....");
									}

								}

							}

							rdd.checkpoint();
							return null;
						}
					});

			ssc.start();
			ssc.awaitTermination();

		} catch (Exception ex) {

			ex.printStackTrace();

		}

	}

	public static void main(String[] args) throws Exception {

		Consumer consumer = new Consumer();
		consumer.run(args);
	}
}
