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

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class PIDControllerTest {

  private int fetchSize;
  private int fillFreqMs;
  private int batchDuration;
  private KafkaConfig config;
  private int queueSize;
  private int fixedRate;

  @Before
  public void setUp() {
    fetchSize = 500; // 500 messages per poll
    fillFreqMs = 1000;        // 1 Seconds Fill Frequency
    batchDuration = 30000;    // 30 seconds batch
    Properties props = new Properties();
    props.setProperty("max.poll.records", Integer.toString(fetchSize));
    props.setProperty("kafka.consumer.id", "1");
    config = new KafkaConfig(props);
    queueSize = 0;
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testPIControllerWithProcessingDelay() {
    long schedulingDelay = 1000;  // 1 Sec scheduling delay
    long processingDelay = 35000; // 35 Sec Processing Delay
    // There will be Rate Reduction from Original _fetchSizeBytes
    PIDController controller = new PIDController(1.0, 1.0, 0);
    controller.controllerStarted = true;
    fixedRate =
      controller.calculateRate(
          config, batchDuration, fetchSize, fillFreqMs,
        schedulingDelay, processingDelay);
    assertEquals(414, fixedRate);
  }

  @Test
  public void testPIControllerWithSchedulingDelay() {
    long schedulingDelay = 20000;  // 20 Sec scheduling delay
    long processingDelay = 30000; // 30 Sec Processing Delay
    // There will be Rate Reduction from Original _fetchSizeBytes
    PIDController controller = new PIDController(1.0, 1.0, 0);
    controller.controllerStarted = true;
    fixedRate =
      controller.calculateRate(
          config, batchDuration, fetchSize, fillFreqMs,
        schedulingDelay, processingDelay);
    assertEquals(166, fixedRate);
  }

  @Test
  public void testPIControllerWithNoDelay() {
    long schedulingDelay = 0;  // 1 Sec scheduling delay
    long processingDelay = 30000; // 30 Sec Processing Delay
    // There will be Rate Reduction from Original _fetchSizeBytes
    PIDController controller = new PIDController(1.0, 1.0, 0);
    controller.controllerStarted = true;
    fixedRate =
      controller.calculateRate(
          config, batchDuration, fetchSize, fillFreqMs,
        schedulingDelay, processingDelay);
    assertEquals(500, fixedRate);
  }

  private int getRate() {
    if (queueSize > config._batchQueueToThrottle) {
      return 10;
    } else {
      if (fixedRate < config._pollRecords) {
        return config._pollRecords;
      }
    }
    return config._pollRecords;
  }

  private void onBatchSubmitted() {
    queueSize++;
    if (queueSize > config._batchQueueToThrottle) {
      fixedRate = 10;
    }
  }

  private void onBatchCompleted() {
    queueSize--;
  }
}
