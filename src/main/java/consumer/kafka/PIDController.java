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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PIDController {

  private double proportional;
  private double integral;
  private double derivative;
  private long latestTime;
  private double latestError = -1;

  public static final Logger LOG = LoggerFactory.getLogger(PIDController.class);

  public PIDController(double proportional, double integral, double derivative) {

    this.proportional = proportional;
    this.integral = integral;
    this.derivative = derivative;
    this.latestTime = System.currentTimeMillis();
  }

  public int calculateRate(
      long time,
      long batchDuration,
      int partitionCount,
      int fetchSizeBytes,
      int fillFreqMs,
      long schedulingDelay,
      long processingDelay) {

    LOG.debug("======== Rate Revision Starts ========");
    double delaySinceUpdate = (time - latestTime);
    int blocksPerSecond = (1000 / fillFreqMs);
    double fixedRate = (partitionCount * fetchSizeBytes) * blocksPerSecond;
    double processingDecay = (double) batchDuration / processingDelay;
    // in bytes / seconds
    double processingRate = ((partitionCount * fetchSizeBytes) * (1000 / fillFreqMs)) * processingDecay;
    // in bytes / seconds
    double error = (double) (fixedRate - processingRate);

    LOG.debug("Fetch Size       : {}", fetchSizeBytes);
    LOG.debug("Fill Freq        : {}", fillFreqMs);
    LOG.debug("Batch Duration   : {}", batchDuration);
    LOG.debug("Partition count  : {}", partitionCount);
    LOG.debug("Scheduling Delay : {}", schedulingDelay);
    LOG.debug("Processing Delay : {}", processingDelay);
    LOG.debug("Fixed Rate       : {}", new Double(fixedRate).intValue());
    LOG.debug("Processing rate  : {}", new Double(processingRate).intValue());
    LOG.debug("Error            : {}", new Double(error).intValue());

    // (in bytes /second)
    double historicalError = (schedulingDelay * processingRate) / 1000;

    LOG.debug("HistoricalError  : {}", new Double(historicalError).intValue());

    // in bytes /(second)
    double dError = (error - latestError) / delaySinceUpdate;

    LOG.debug("Derror           : " + dError);

    double newRate = (fixedRate 
        - proportional * error
        - integral * historicalError
        - derivative * dError);

    newRate = (newRate / partitionCount) / blocksPerSecond;

    LOG.debug("Reviced   Fetch  : {}", new Double(newRate).intValue());

    if (newRate > fetchSizeBytes) {
      newRate = fetchSizeBytes;
    }

    latestError = error;
    int rate = new Double(newRate).intValue();
    LOG.debug("New Fetch Size   : {}", rate);
    LOG.debug("Percent Change   : {}", ((double) (fetchSizeBytes - rate) / fetchSizeBytes) * 100);
    LOG.debug("======== Rate Revision Ends ========");

    return new Double(newRate).intValue();
  }

  public static void main(String[] args) {

    int _fetchSizeBytes = 1048576; // 1024 * 1024
    int _fillFreqMs = 250;
    int batchDuration = 5000; // 5 Seconds
    int topicPartition = 3;

    long schedulingDelay = 1000; // 1 Sec scheduling delay
    long processingDelay = 6000; // 6 Sec Processing Delay

    long time = System.currentTimeMillis() + 250;

    PIDController controller = new PIDController(0.75, 0.15, 0);

    // There will be Rate Reduction from Original _fetchSizeBytes
    controller.calculateRate(
        time,
        batchDuration,
        topicPartition,
        _fetchSizeBytes,
        _fillFreqMs,
        schedulingDelay,
        processingDelay);
  }
}
