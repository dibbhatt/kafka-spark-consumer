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
  private int lastBatchFetchPercentage = 0;
  public long lastBatchRecords = 1;
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
    KafkaConfig config, long batchDurationMs, int partitionCount, int fetchSizeBytes,
    int fillFreqMs, long schedulingDelayMs, long processingDelayMs, long numRecords) {

    long time = System.currentTimeMillis();
    double delaySinceUpdate = (time - latestTime);

    //Safe guard 90% of Batch Duration
    batchDurationMs = (long) ((long)batchDurationMs * config._safeBatchPercent);

    //Every Fill create one Spark Block
    double blocksPerSecond = 1000 / (double)fillFreqMs;

    //Incoming fetchSizeBytes set to 1KB due to Queue size increase.
    //Let not consider that to calculate  next rate
    if(fetchSizeBytes == 1024) {
      fetchSizeBytes = config._fetchSizeBytes;
    }

    //Pull Rate bytes / seconds
    double fixedRate = (partitionCount * fetchSizeBytes) * blocksPerSecond;

    double processingDecay = 1.0;

    //Decay in processing
    if(processingDelayMs > 0) {
      processingDecay = batchDurationMs / (double) processingDelayMs;
    }

    // Processing Rate in bytes / seconds
    double processingRate = partitionCount * fetchSizeBytes * blocksPerSecond  * processingDecay;

    //Proportional Error = Fixed Rate - Processing Rate in bytes / seconds
    double proportationalError = (double) (fixedRate - processingRate);

    //Historical Error = Scheduling Delay * Processing Rate / Batch Duration (in bytes /second)
    double historicalError = (schedulingDelayMs * processingRate) / batchDurationMs;

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
    double revisedFetchSize = (revisedRate / partitionCount) / blocksPerSecond;
    //Predicted next batch fetch percentage
    int nextBatchFetchPercentage = (int)(((double)(((revisedFetchSize - fetchSizeBytes) / fetchSizeBytes)) * 100));
    //Max allowable change is 20 %.
    //Cap the max change to avoid overshoot
    if(Math.abs(nextBatchFetchPercentage) > (int)(config._maxRateChangePercent * 100)) {
      if(nextBatchFetchPercentage > 0) {
        revisedFetchSize = fetchSizeBytes + fetchSizeBytes * config._maxRateChangePercent;
        nextBatchFetchPercentage = (int)(config._maxRateChangePercent * 100);
      } else {
        revisedFetchSize = fetchSizeBytes - fetchSizeBytes * config._maxRateChangePercent;
        nextBatchFetchPercentage = - (int)(config._maxRateChangePercent * 100);
      }
    }

    LOG.info("======== Rate Revision Starts ========");
    LOG.info("Current Fetch Size    : " + fetchSizeBytes);
    LOG.info("Fill Freq             : " + fillFreqMs);
    LOG.info("Batch Duration        : " + batchDurationMs);
    LOG.info("Partition count       : " + partitionCount);
    LOG.info("Scheduling Delay      : " + schedulingDelayMs);
    LOG.info("Processing Delay      : " + processingDelayMs);
    LOG.info("Fixed Rate            : " + (int)fixedRate);
    LOG.info("Processing rate       : " + (int)processingRate);
    LOG.info("Proportional Error    : " + (int)proportationalError);
    LOG.info("HistoricalError       : " + (int)historicalError);
    LOG.info("DifferentialError     : " + (int)differentialError);
    LOG.info("Reviced Rate          : " + (int)revisedRate);
    LOG.info("Proposed FetchPercent : " + (int)nextBatchFetchPercentage);

    if(!controllerStarted) {
      //warm up controller for first iteration
      controllerStarted = true;
      lastBatchRecords = numRecords;
      lastBatchFetchPercentage = nextBatchFetchPercentage;
      revisedFetchSize = config._fetchSizeBytes;
    } else {
      // Check if predicted rate can be applied based on historical changes
      // If predicted percentage > 0, check if number of records fetched in equal proportion
     if(nextBatchFetchPercentage > 0) {
        int currentBatchRecordPercentage = (int)(((((numRecords - lastBatchRecords) / (double)lastBatchRecords)) * 100));
        LOG.info("Last FetchPercent     : " + (int)lastBatchFetchPercentage);
        LOG.info("Current RecordPercent : " + (int)currentBatchRecordPercentage);
        //Consumed records in this batch is higher than earlier batch
        if(currentBatchRecordPercentage > 0 && currentBatchRecordPercentage < nextBatchFetchPercentage ) {
          revisedFetchSize = fetchSizeBytes + fetchSizeBytes * currentBatchRecordPercentage / 100;
          nextBatchFetchPercentage = currentBatchRecordPercentage;
          //Lower number of records fetched from earlier batch. Let lower the rate
        } else if (currentBatchRecordPercentage <= 0) {
          revisedFetchSize = fetchSizeBytes + fetchSizeBytes * currentBatchRecordPercentage / 100;
          //If this goes below configured _fetchSizeBytes , floor it
          if(revisedFetchSize < config._fetchSizeBytes) {
            revisedFetchSize = config._fetchSizeBytes;
            nextBatchFetchPercentage = 0;
          } else {
            nextBatchFetchPercentage = currentBatchRecordPercentage;
          }
        }
      }

      LOG.info("Last FetchRecords     : " + (int)lastBatchRecords);
      LOG.info("Current FetchRecords  : " + (int)numRecords);
      //These will be used for next batch
      lastBatchRecords = (numRecords > 0) ? numRecords : 1;
      lastBatchFetchPercentage = nextBatchFetchPercentage;
    }
    LOG.info("Reviced FetchSize     : " + (int)revisedFetchSize);
    LOG.info("Reviced PercentChange : " + nextBatchFetchPercentage ) ;
    LOG.info("======== Rate Revision Ends ========");
    latestError = proportationalError;
    latestTime = time;
    return new Double(revisedFetchSize).intValue();
  }
}
