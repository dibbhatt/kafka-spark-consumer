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

  public static <T> JavaPairDStream<Integer, Iterable<Long>> getPartitionOffset(
      JavaDStream<MessageAndMetadata<T>> unionStreams, Properties props) {
    JavaPairDStream<Integer, Long> partitonOffsetStream = unionStreams.mapPartitionsToPair(new PartitionOffsetPair<>());
    JavaPairDStream<Integer, Iterable<Long>> partitonOffset = partitonOffsetStream.groupByKey(1);
    return partitonOffset;
  }

  @SuppressWarnings("deprecation")
  public static void persists(JavaPairDStream<Integer, Iterable<Long>> partitonOffset, Properties props) {
    partitonOffset.foreachRDD(new VoidFunction<JavaPairRDD<Integer,Iterable<Long>>>() {
      @Override
      public void call(JavaPairRDD<Integer, Iterable<Long>> po) throws Exception {
        List<Tuple2<Integer, Iterable<Long>>> poList = po.collect();
        doPersists(poList, props);
      }
    });
  }

  public static <T> DStream<Tuple2<Integer, Iterable<Long>>>  getPartitionOffset(
      DStream<MessageAndMetadata<T>> unionStreams, Properties props) {
    ClassTag<MessageAndMetadata<T>> messageMetaClassTag = 
        ScalaUtil.<T>getMessageAndMetadataClassTag();
    JavaDStream<MessageAndMetadata<T>> javaDStream = 
        new JavaDStream<MessageAndMetadata<T>>(unionStreams, messageMetaClassTag);
    JavaPairDStream<Integer, Iterable<Long>> partitonOffset = getPartitionOffset(javaDStream, props);
    return partitonOffset.dstream();
  }

  @SuppressWarnings("deprecation")
  public static void persists(DStream<Tuple2<Integer, Iterable<Long>>> partitonOffset, Properties props) {
    ClassTag<Tuple2<Integer, Iterable<Long>>> tuple2ClassTag = 
        ScalaUtil.<Integer, Iterable<Long>>getTuple2ClassTag();
    JavaDStream<Tuple2<Integer, Iterable<Long>>> jpartitonOffset = 
        new JavaDStream<Tuple2<Integer, Iterable<Long>>>(partitonOffset, tuple2ClassTag);
    jpartitonOffset.foreachRDD(new VoidFunction<JavaRDD<Tuple2<Integer, Iterable<Long>>>>() {
      @Override
      public void call(JavaRDD<Tuple2<Integer, Iterable<Long>>> po) throws Exception {
        List<Tuple2<Integer, Iterable<Long>>> poList = po.collect();
        doPersists(poList, props);
      }
    });
  }

  private static void doPersists(List<Tuple2<Integer, Iterable<Long>>> poList, Properties props) {
    Map<Integer, Long> partitionOffsetMap = new HashMap<Integer, Long>();
    for(Tuple2<Integer, Iterable<Long>> tuple : poList) {
      int partition = tuple._1();
      Long offset = getMaximum(tuple._2());
      partitionOffsetMap.put(partition, offset);
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

  private static void persistProcessedOffsets(Properties props, Map<Integer, Long> partitionOffsetMap) {
    ZkState state = new ZkState(props.getProperty(Config.ZOOKEEPER_CONSUMER_CONNECTION));
    for(Map.Entry<Integer, Long> po : partitionOffsetMap.entrySet()) {
      String path = processedPath(po.getKey(), props);
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

  public static String processedPath(int partition, Properties props) {
    String consumerZkPath = "/consumers";
    if (props.getProperty("zookeeper.consumer.path") != null) {
      consumerZkPath = props.getProperty("zookeeper.consumer.path");
    }
    return consumerZkPath +  "/"
      + props.getProperty(Config.KAFKA_CONSUMER_ID)
      + "/processed/"
      + props.getProperty(Config.KAFKA_TOPIC) + "/"
      + partition;
  }
}
