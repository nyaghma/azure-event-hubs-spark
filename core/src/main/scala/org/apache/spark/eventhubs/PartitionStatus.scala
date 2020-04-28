/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.eventhubs


private[eventhubs] class PartitionStatus(val nAndP: NameAndPartition, val maxBatchReceiveTime: Long) {

  // request sequence number (start sequence number) of the current batch
  private var currentBatchRequestSeqNo: SequenceNumber = -1

  // request sequence number (start sequence number) of the last recieved performance update
  private var lastUpdateRequestSeqNo: SequenceNumber = -1

  // batch size of the last recieved performance update
  private var lastUpdateBatchSize: Int = -1

  // total receive time for the batch from the last recieved performance update
  private var lastUpdateReceiveTimeInMillis: Long = -1

  // Update the current batch that is being executed
  def updateCurrentBatch(curBatchReqSeqNo: SequenceNumber) = {
    if(curBatchReqSeqNo < this.currentBatchRequestSeqNo)
      throw new IllegalStateException(s"Can not update the current batch with startSeqNo=$currentBatchRequestSeqNo to an older batch with StartSeqNo=$curBatchReqSeqNo")
    this.currentBatchRequestSeqNo = curBatchReqSeqNo
  }

  // Update the status of this partition with the latest performance metrics
  def updatePerformanceMetrics(requestSeqNo: SequenceNumber, batchSize: Int, receiveTimeInMillis: Long): Boolean = {
    // Should not receive update for a batch with requestSeqNo greater than currentBatchRequestSeqNo
    if(requestSeqNo > this.currentBatchRequestSeqNo)
      throw new IllegalStateException(s"Should not receive update for a batch with startSeqNo=$requestSeqNo greater than the current batch with StartSeqNo=$currentBatchRequestSeqNo")

    // discard performance metrics for old batches (batches with requestSeqNo less than lastUpdateRequestSeqNo)
    if(requestSeqNo < this.lastUpdateRequestSeqNo)
      false

    // update the partition status
    this.lastUpdateRequestSeqNo = requestSeqNo
    this.lastUpdateBatchSize = batchSize
    this.lastUpdateReceiveTimeInMillis = receiveTimeInMillis
    true
  }

  // check if the partition is slow or not based on the latest received performacne metrics
  def isSlow: Boolean = {
    if(this.lastUpdateReceiveTimeInMillis > this.maxBatchReceiveTime)
      true
    else
      false
  }

  //
  def maxAllowedEventsInNextBatch: Int = {
    if(!this.isSlow)
      Int.MaxValue

    if(this.lastUpdateBatchSize == 0)
      throw new IllegalStateException(s"BatchSize should not be zero.")

    val receiveTimePerEvent: Float = this.lastUpdateReceiveTimeInMillis.toFloat / this.lastUpdateBatchSize
    val maxAllowedEvents: Float = this.maxBatchReceiveTime.toFloat / receiveTimePerEvent
    maxAllowedEvents.floatValue().toInt
  }
}