/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consumer.kafka.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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

import consumer.kafka.KafkaConfig;
import consumer.kafka.MessageAndMetadata;

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

		SparkConf _sparkConf = new SparkConf().setAppName("KafkaReceiver")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true");;

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

						@Override
						public Void call(JavaRDD<MessageAndMetadata> rdd,
								Time time) throws Exception {

							for (MessageAndMetadata record : rdd.collect()) {

								if (record != null) {

									System.out.println(new String(record
											.getPayload()));

								}

							}
							return null;
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
