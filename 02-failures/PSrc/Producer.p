/* 
The Producer will write all its log entries to the log nodes.
*/
machine Producer {
    // The current sequence number we try to write to the log.
    var currentEntry: int;
    // The current acknowledgements we got from the log nodes for the current entry we try to write to the log.
    var appendAcks: set[LogNode];
    // Total number of log entries the producer should write to the log.
    var numEntries: int;
    var coordinator: Coordinator;
    var timer: Timer;
    // Identifies the log we want to write to. For now we keep this constant in a producer. In a real system this 
    // could be a shard of a log.
    var logId: tLogId;
    // The current segment we send log entries to
    var currentSegmentId: tSegmentId;
    // The current log nodes we send new log entries to
    var currentNodes: set[LogNode];
    // Keeps track of how many log entries were acknowledged from all nodes in the current segment. The idea is once
    // we close the current segment, we remove all partially acknowledged log entries by sending this number to all 
    // log nodes in the close message. The log nodes then truncate any partially replicated log entries. And it is then
    // safe to return all log entries of the segment (including the last one) to the consumers vs. usually we don't 
    // include the last log entry because it could be only partially committed.
    var currentSegmentEntries: int;

    start state Init {
        entry (payload: (logId: tLogId, coordinator: Coordinator, numEntries: int)) {
            numEntries = payload.numEntries;
            coordinator = payload.coordinator;
            logId = payload.logId;
            currentEntry = 0;
            currentSegmentEntries = 0;

            // indicates that there is no open log segment currently
            currentSegmentId = -1;
            timer = CreateTimer(this);
            
            goto InitNewSegment;
        }
    }

    state InitNewSegment {
        entry {
            UnreliableSend(coordinator, eNewSegment, (client = this, logId = logId, previousSegment = currentSegmentId, previousSegmentNumEntries = currentSegmentEntries));
            // We use a timer to retry this request. It could be that the coordinator does not reply in time to our request. For instance,
            // it could be that no log node replies in time to the coordinator that the previous segment is closed.
            StartTimer(timer);
        }

        on eNewSegmentResponse do (response: tNewSegmentResponse) {
            // The reply is not for our current request
            if (response.newSegment != currentSegmentId + 1) {
                return;
            }

            // For this implementation, it does not make a difference if it is NEW_SEG_OK or ALREADY_APPENDED
            // (which we get for a retry). If we want to consider multiple producers, we should check in addition
            // that no other producer has created this segment.
            assert response.status == NEW_SEG_OK || response.status == NEW_SEG_ALREADY_APPENDED;

            currentSegmentId = response.newSegment;
            currentNodes = response.nodes;
            currentSegmentEntries = 0;
            goto Produce;
        }

        on eTimeOut do {
            goto InitNewSegment;
        }

        ignore eAppendResponse;
    }

    state Produce {
        entry {
            var segmentKey: tSegmentKey;
            segmentKey = (logId = logId, segmentId = currentSegmentId);
            BroadcastToNodes(currentNodes, eAppendRequest, (client = this, segmentKey = segmentKey, seqNum = currentEntry, val = currentEntry));
            print format("Producer tries to send sequence num {0} to segment {1} (nodes: {2})", currentEntry, segmentKey.segmentId, currentNodes);
            StartTimer(timer);
        }

        on eAppendResponse do (response: tAppendResponse) {
            if (response.segmentKey.segmentId != currentSegmentId) {
                return;
            }

            // The segment is closed. We should open a new one.
            if (response.status == APPEND_SEG_CLOSED && response.seqNum == currentEntry) {
                goto InitNewSegment;
            }

            // APPEND_OK received from a log node
            assert response.status == APPEND_OK;
            appendAcks += (response.logNode);
            if (sizeof(appendAcks) < sizeof(currentNodes)) {
                return;
            }

            // Received acknowledgements from all log nodes. In this case we can move on to the next log entry.
            appendAcks = default(set[LogNode]);
            currentSegmentEntries = currentSegmentEntries + 1;
            currentEntry = currentEntry + 1;
            
            // In a real implementation, we could acknowledge the log write to any client submitting log entries
            // to the producer.
            announce eLogEntryCommitted, (seqNum = response.seqNum, logId = logId);
            print format("Producer committed sequence number {0} (segment: {1})", response.seqNum, response.segmentKey.segmentId);

            if (currentEntry == numEntries) {
                goto Done;
            }
            
            goto Produce;
        }

        on eTimeOut do {
            appendAcks = default(set[LogNode]);
            goto InitNewSegment;
        }

        ignore eNewSegmentResponse;
    }

    state Done {
        entry {
            // This is to shutdown the entire simulation once producers and consumers are done
            send coordinator, eProducersDone;
            send timer, halt;
        }
        ignore eAppendResponse, eTimeOut, eNewSegmentResponse;
    }
}
