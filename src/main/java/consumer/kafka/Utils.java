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

/*
 *   This file is based on the source code of the Kafka spout of the Apache Storm project.
 *   (https://github.com/apache/storm/tree/master/external/storm-kafka)
 *   This file has been modified to work with Spark Streaming.
 */

package consumer.kafka;

import java.nio.ByteBuffer;

public class Utils {

  public static Integer getInt(Object o) {
    if (o instanceof Long) {
      return ((Long) o).intValue();
    } else if (o instanceof Integer) {
      return (Integer) o;
    } else if (o instanceof Short) {
      return ((Short) o).intValue();
    } else {
      throw new IllegalArgumentException("Don't know how to convert "
          + o + " + to int");
    }
  }

  public static byte[] toByteArray(ByteBuffer buffer) {
    byte[] ret = new byte[buffer.remaining()];
    buffer.get(ret, 0, ret.length);
    return ret;
  }

  public static void setFetchRate(KafkaConfig config, Integer rate) {
    String path = config._stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH)
        + "/" + config._stateConf.get(Config.KAFKA_CONSUMER_ID) + "/newrate";
    ZkState state = new ZkState((String) config._stateConf
            .get(Config.ZOOKEEPER_CONSUMER_CONNECTION));
    state.writeBytes(path, rate.toString().getBytes());
    state.close();
  }
}
