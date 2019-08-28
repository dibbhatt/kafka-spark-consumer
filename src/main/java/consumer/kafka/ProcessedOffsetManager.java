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

package consumer.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class ProcessedOffsetManager<T> {

  private static final Log LOG = LogFactory.getLog(ProcessedOffsetManager.class);

  public static <T> JavaPairDStream<String, Iterable<Long>> getPartitionOffset(
      JavaDStream<MessageAndMetadata<T>> unionStreams, Properties props) {
    JavaPairDStream<String, Long> partitonOffsetStream = unionStreams.mapPartitionsToPair(new PartitionOffsetPair<>());
    JavaPairDStream<String, Iterable<Long>> partitonOffset = partitonOffsetStream.groupByKey(1);
    return partitonOffset;
  }

  @SuppressWarnings("deprecation")
  public static void persists(JavaPairDStream<String, Iterable<Long>> partitonOffset, Properties props) {
    partitonOffset.foreachRDD(new VoidFunction<JavaPairRDD<String,Iterable<Long>>>() {
      @Override
      public void call(JavaPairRDD<String, Iterable<Long>> po) throws Exception {
        List<Tuple2<String, Iterable<Long>>> poList = po.collect();
        doPersists(poList, props);
      }
    });
  }

  // added to avoid skipped message due to exception thrown during processing RDD
  @SuppressWarnings("deprecation")
  public static <T> void persistsPartition(JavaRDD<MessageAndMetadata<T>> rdd, Properties props) throws Exception {
        JavaPairRDD<String,Long> partitionOffsetRdd = rdd.mapPartitionsToPair(new PartitionOffsetPair<>());
        JavaPairRDD<String, Iterable<Long>> partitonOffset = partitionOffsetRdd.groupByKey(1);
        List<Tuple2<String, Iterable<Long>>> poList = partitonOffset.collect();
        doPersists(poList, props);
  }


  public static <T> DStream<Tuple2<String, Iterable<Long>>>  getPartitionOffset(
      DStream<MessageAndMetadata<T>> unionStreams, Properties props) {
    ClassTag<MessageAndMetadata<T>> messageMetaClassTag =
        ScalaUtil.<T>getMessageAndMetadataClassTag();
    JavaDStream<MessageAndMetadata<T>> javaDStream =
        new JavaDStream<MessageAndMetadata<T>>(unionStreams, messageMetaClassTag);
    JavaPairDStream<String, Iterable<Long>> partitonOffset = getPartitionOffset(javaDStream, props);
    return partitonOffset.dstream();
  }

  @SuppressWarnings("deprecation")
  public static void persists(DStream<Tuple2<String, Iterable<Long>>> partitonOffset, Properties props) {
    ClassTag<Tuple2<String, Iterable<Long>>> tuple2ClassTag =
        ScalaUtil.<String, Iterable<Long>>getTuple2ClassTag();
    JavaDStream<Tuple2<String, Iterable<Long>>> jpartitonOffset =
        new JavaDStream<Tuple2<String, Iterable<Long>>>(partitonOffset, tuple2ClassTag);
    jpartitonOffset.foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, Iterable<Long>>>>() {
      @Override
      public void call(JavaRDD<Tuple2<String, Iterable<Long>>> po) throws Exception {
        List<Tuple2<String, Iterable<Long>>> poList = po.collect();
        doPersists(poList, props);
      }
    });
  }

  private static void doPersists(List<Tuple2<String, Iterable<Long>>> poList, Properties props) {
    Map<String, Long> partitionOffsetMap = new HashMap<String, Long>();
    for(Tuple2<String, Iterable<Long>> tuple : poList) {
      Long offset = getMaximum(tuple._2());
      partitionOffsetMap.put(tuple._1(), offset);
    }
    persistProcessedOffsets(props, partitionOffsetMap);
  }

  private static <T extends Comparable<T>> T getMaximum(Iterable<T> values) {
    T max = null;
    for (T value : values) {
      if (max == null || max.compareTo(value) < 0) {
          max = value;
      }
    }
    return max;
  }

  private static void persistProcessedOffsets(Properties props, Map<String, Long> partitionOffsetMap) {
    String zkPath = getZKPath(props);
    ZkState state = new ZkState(zkPath);
    for(Map.Entry<String, Long> po : partitionOffsetMap.entrySet()) {
      String[] tp = po.getKey().split(":");
      String path = processedPath(tp[0], Integer.parseInt(tp[1]), props);
      try{
        state.writeBytes(path, po.getValue().toString().getBytes());
        LOG.info("Wrote processed offset " + po.getValue() + " for Parittion " + po.getKey());
      }catch (Exception ex) {
        LOG.error("Error while comitting processed offset " + po.getValue() + " for Parittion " + po.getKey(), ex);
        state.close();
        throw ex;
      }
    }
    state.close();
  }

  private static String getZKPath(Properties props) {
    //ZK Host and Port for Kafka Cluster
    String zkHost = props.getProperty("zookeeper.hosts");
    String zkPort = props.getProperty("zookeeper.port");
    //ZK host:port details for Offset writing
    String consumerConnection = "";
    if(props.getProperty("zookeeper.consumer.connection") != null) {
      consumerConnection = props.getProperty("zookeeper.consumer.connection");
    } else {
      String[] zkh = zkHost.split(",");
      for(String host: zkh) {
        String hostport = host + ":" + zkPort;
        consumerConnection = consumerConnection + "," + hostport;
      }
      consumerConnection = consumerConnection.substring(consumerConnection.indexOf(',')+1);
    }
    return consumerConnection;
  }

  public static String processedPath(String topic, int partition, Properties props) {
    String consumerZkPath = "/consumers";
    if (props.getProperty("zookeeper.consumer.path") != null) {
      consumerZkPath = props.getProperty("zookeeper.consumer.path");
    }
    return consumerZkPath +  "/"
      + props.getProperty(Config.KAFKA_CONSUMER_ID)
      + "/offsets/"
      + topic + "/"
      + partition;
  }
}
