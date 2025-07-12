/* 
Reads the log entries sequentially. Will retry if no new log entries were reported
yet.
*/
machine Consumer {
    // The next offset to be consumed from the segment
    var currentOffset: int;

    // State of the current segment the consumer reads from
    var currentSegmentId: tSegmentId;
    var currentNodes: set[LogNode];

    var logId: tLogId;
    var coordinator: Coordinator;
    var timer: Timer;
    var totalNumLogEntries: int;
    // The last sequence number that was consumed
    var lastSeqNum: int;
    // When we have read up to that sequence number, we can stop
    var finalSeqNum: int;

    start state Init {
        entry (payload: (logId: tLogId, totalNumLogEntries: int, coordinator: Coordinator, finalSeqNum: int)) {
            timer = CreateTimer(this);
            lastSeqNum = -1;
            coordinator = payload.coordinator;
            logId = payload.logId;
            currentSegmentId = 0;
            totalNumLogEntries = payload.totalNumLogEntries;
            finalSeqNum = payload.finalSeqNum;
            goto FindSegmentNodes;
        }
    }

    state FindSegmentNodes {
        entry {
            print format("Consumer tries to find segment {0}", currentSegmentId);
            // Timer is needed to retry finding the new segment
            CancelTimer(timer);
            StartTimer(timer);
            UnreliableSend(coordinator, eSegmentState, (client = this, logId = logId, segment = currentSegmentId));
        }

        on eSegmentStateResponse do (response: tSegmentStateResponse) {
            if (response.segment != currentSegmentId) {
                return;
            }
            if (response.status == SEG_NOT_INIT) {
                goto FindSegmentNodes;
            }
            assert response.status == SEG_OPEN;

            currentNodes = response.nodes;
            goto Consume;
        }

        on eTimeOut goto FindSegmentNodes;

        ignore eReadResponse;
    }

    state Consume {
        entry {
            var segmentKey: tSegmentKey;
            segmentKey = (logId = logId, segmentId = currentSegmentId);
            UnreliableSend(choose(currentNodes), eReadRequest, (client = this, segmentKey = segmentKey, offset = currentOffset));
            // Timer is needed to retry the read (the first read could for instance return 
            // that there are no log entries to consume)
            StartTimer(timer);
        }

        on eReadResponse do (response: tReadResponse) {
            if (response.segmentKey.segmentId != currentSegmentId) {
                return;
            }
            if (response.offset != currentOffset) {
                return;
            }

            CancelTimer(timer);

            // Log node informs that a new segment needs to be read
            if (response.status == READ_NEW_SEGMENT) {
                currentOffset = 0;
                currentSegmentId = currentSegmentId + 1;
                goto FindSegmentNodes;
            }

            if (response.status == READ_OK) {
                print format("Consumer got sequence number {0}", response.seqNum);
                // Ensure monotonic sequence numbers without jumps
                // However, because at a random point a new producer can take over (and then produces 
                // log entries not in the range 0-9 but 10-19) we allow jumps if the sequence number is
                // divisible by 10.
                if (response.seqNum % (totalNumLogEntries + 1) != 0) {
                    assert response.seqNum == lastSeqNum || response.seqNum == lastSeqNum + 1;
                }
                
                currentOffset = currentOffset + 1;

                // Check if the log entry is actually new
                if (response.seqNum >= lastSeqNum + 1) {
                    lastSeqNum = response.seqNum;   
                }

                if (response.seqNum == finalSeqNum) {
                    goto Done;
                }
            }
            // Nothing to do for READ_NO_MORE_ENTRIES
            goto Consume;
        }

        on eTimeOut do {
            goto Consume;
        }
        ignore eSegmentStateResponse;
    }

    state Done {
        entry {
            // Similarly to the producer, this is to shutdown the simulation once 
            // producers and consumers are done
            send coordinator, eConsumersDone;
            send timer, halt;
        }
        
        ignore eSegmentStateResponse, eReadResponse, eTimeOut;
    }
}
