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
  public boolean controllerStarted;

  public static final Logger LOG = LoggerFactory.getLogger(PIDController.class);

  public PIDController(double proportional, double integral, double derivative) {

    this.proportional = proportional;
    this.integral = integral;
    this.derivative = derivative;
    this.latestTime = System.currentTimeMillis();
    this.controllerStarted = false;
  }

  public int calculateRate(
    KafkaConfig config, long batchDurationMs, int pollSize,
    int fillFreqMs, long schedulingDelayMs, long processingDelayMs) {

    long time = System.currentTimeMillis();
    double delaySinceUpdate = (time - latestTime);

    //Fixed rate mesages/sec
    double fixedRate = (pollSize / (double)fillFreqMs ) * 1000;

    double processingRate = fixedRate;
    if(processingDelayMs > 0) {
      double processingDecay = batchDurationMs / (double)processingDelayMs;
      processingRate = fixedRate * processingDecay ;
    }

    //Pull Rate bytes / seconds
    System.out.println("processingRate rate " + processingRate);
    System.out.println("fixed rate " + fixedRate);

    //Proportional Error = Fixed Rate - Processing Rate in records / seconds
    double proportationalError = fixedRate - processingRate;
    System.out.println("P "+proportationalError);

    //Historical Error = Scheduling Delay * Processing Rate / Batch Duration (in records /second)
    double historicalError = (schedulingDelayMs * processingRate) / batchDurationMs;

    System.out.println("H "+historicalError);
    
    //Differential Error. Error Rate is changing (in bytes /second)
    double differentialError = 0.0;

    if(delaySinceUpdate > 0) {
      differentialError = (proportationalError - latestError) / delaySinceUpdate; 
    }

    double revisedRate = (fixedRate
        - proportional * proportationalError
        - integral * historicalError
        - derivative * differentialError);

    //Predicted next batch fetch rate
    double revisedFetchSize = revisedRate;

    LOG.info("======== Rate Revision Starts ========");
    LOG.info("Current Fetch Size    : " + pollSize);
    LOG.info("Fill Freq             : " + fillFreqMs);
    LOG.info("Batch Duration        : " + batchDurationMs);
    LOG.info("Scheduling Delay      : " + schedulingDelayMs);
    LOG.info("Processing Delay      : " + processingDelayMs);
    LOG.info("Fixed Rate            : " + (int)fixedRate);
    LOG.info("Processing rate       : " + (int)processingRate);
    LOG.info("Proportional Error    : " + (int)proportationalError);
    LOG.info("HistoricalError       : " + (int)historicalError);
    LOG.info("DifferentialError     : " + (int)differentialError);
    LOG.info("Reviced Rate          : " + (int)revisedRate);

    if(!controllerStarted) {
      //warm up controller for first iteration
      controllerStarted = true;
      revisedFetchSize = config._pollRecords;
    }

    LOG.info("Reviced FetchSize     : " + (int)revisedFetchSize);
    LOG.info("======== Rate Revision Ends ========");
    latestError = proportationalError;
    latestTime = time;
    return new Double(revisedFetchSize).intValue();
  }
}
