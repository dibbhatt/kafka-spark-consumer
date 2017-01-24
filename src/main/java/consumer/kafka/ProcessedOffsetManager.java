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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;

import scala.Tuple2;
import scala.reflect.ClassTag;

import com.google.common.collect.ImmutableMap;

public class ProcessedOffsetManager {

  public static JavaPairDStream<Integer, Iterable<Long>> getPartitionOffset(JavaDStream<MessageAndMetadata> unionStreams) {
    JavaPairDStream<Integer, Long> partitonOffsetStream = unionStreams.mapPartitionsToPair
        (new PairFlatMapFunction<Iterator<MessageAndMetadata>, Integer, Long>() {
          @Override
          public Iterable<Tuple2<Integer, Long>> call(Iterator<MessageAndMetadata> entry) throws Exception {
            MessageAndMetadata mmeta = null;
            List<Tuple2<Integer, Long>> l = new ArrayList<Tuple2<Integer, Long>>();
            while(entry.hasNext()) {
              mmeta = entry.next();
            }
            if(mmeta != null) {
              l.add(new Tuple2<Integer, Long>(mmeta.getPartition().partition,mmeta.getOffset()));
            }
            return l;
          }
    });
    JavaPairDStream<Integer, Iterable<Long>> partitonOffset = partitonOffsetStream.groupByKey(1);
    return partitonOffset;
  }

  @SuppressWarnings("deprecation")
  public static void persists(JavaPairDStream<Integer, Iterable<Long>> partitonOffset, Properties props) {
    partitonOffset.foreachRDD(new Function<JavaPairRDD<Integer,Iterable<Long>>, Void>() {
      @Override
      public Void call(JavaPairRDD<Integer, Iterable<Long>> po) throws Exception {
        List<Tuple2<Integer, Iterable<Long>>> poList = po.collect();
        doPersists(poList, props);
        return null;
      }
    });
  }

  public static DStream<Tuple2<Integer, Iterable<Long>>>  getPartitionOffset(DStream<MessageAndMetadata> unionStreams) {
    ClassTag<MessageAndMetadata> messageMetaClassTag = 
        ScalaUtil.<MessageAndMetadata>getClassTag(MessageAndMetadata.class);
    JavaDStream<MessageAndMetadata> javaDStream = 
        new JavaDStream<MessageAndMetadata>(unionStreams, messageMetaClassTag);
    JavaPairDStream<Integer, Iterable<Long>> partitonOffset = getPartitionOffset(javaDStream);
    return partitonOffset.dstream();
  }

  @SuppressWarnings("deprecation")
  public static void persists(DStream<Tuple2<Integer, Iterable<Long>>> partitonOffset, Properties props) {
    ClassTag<Tuple2<Integer, Iterable<Long>>> tuple2ClassTag = 
        ScalaUtil.<Integer, Iterable<Long>>getTuple2ClassTag();
    JavaDStream<Tuple2<Integer, Iterable<Long>>> jpartitonOffset = 
        new JavaDStream<Tuple2<Integer, Iterable<Long>>>(partitonOffset, tuple2ClassTag);
    jpartitonOffset.foreachRDD(new Function<JavaRDD<Tuple2<Integer, Iterable<Long>>>, Void>() {
      @Override
      public Void call(JavaRDD<Tuple2<Integer, Iterable<Long>>> po) throws Exception {
        List<Tuple2<Integer, Iterable<Long>>> poList = po.collect();
        doPersists(poList, props);
        return null;
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
      Map<Object, Object> data = (Map<Object, Object>) ImmutableMap
          .builder()
          .put("consumer",ImmutableMap.of("id",props.getProperty(Config.KAFKA_CONSUMER_ID)))
          .put("offset", po.getValue())
          .put("partition",po.getKey())
          .put("broker",ImmutableMap.of("host", "", "port", ""))
          .put("topic", props.getProperty(Config.KAFKA_TOPIC)).build();
      String path = processedPath(po.getKey(), props);
      try{
        state.writeJSON(path, data);
      }catch (Exception ex) {
        state.close();
        throw ex;
      }
    }
    state.close();
  }

  private static String processedPath(int partition, Properties props) {
    return props.getProperty(Config.ZOOKEEPER_CONSUMER_PATH)
      + "/" + props.getProperty(Config.KAFKA_CONSUMER_ID) + "/"
      + props.getProperty(Config.KAFKA_TOPIC)
      + "/processed/" + "partition_"+ partition;
  }
}
