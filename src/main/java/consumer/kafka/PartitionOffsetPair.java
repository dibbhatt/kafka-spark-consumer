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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


/**
 * Extracts the kafka-paritition-number and largest-offset-read-for-that-partition from the kafka-receiver output
 */
@SuppressWarnings("serial")
public class PartitionOffsetPair<E> implements PairFlatMapFunction<Iterator<MessageAndMetadata<E>>, String, Long> {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionOffsetPair.class);

    @Override
    public Iterator<Tuple2<String, Long>> call(Iterator<MessageAndMetadata<E>> it) throws Exception {
        MessageAndMetadata<E> mmeta = null;
        List<Tuple2<String, Long>> kafkaPartitionToOffsetList = new LinkedList<>();
        Map<String, Long> offsetMap = new HashMap<>();
        while (it.hasNext()) {
            mmeta = it.next();
            if (mmeta != null) {
              Long offset = offsetMap.get(mmeta.getTopic() + ":" + mmeta.getPartition().partition);
              if(offset == null) {
                offsetMap.put(mmeta.getTopic() + ":" + mmeta.getPartition().partition, mmeta.getOffset());
              } else {
                if(mmeta.getOffset() > offset) {
                  offsetMap.put(mmeta.getTopic() + ":" + mmeta.getPartition().partition, mmeta.getOffset());
                }
              }
           }
        }
        for(Map.Entry<String, Long> entry : offsetMap.entrySet()) {
          kafkaPartitionToOffsetList.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
        return kafkaPartitionToOffsetList.iterator();
    }
}

