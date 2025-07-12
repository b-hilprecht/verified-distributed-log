type tLogValue = int;
type tLogSeqNum = int;
type tLogId = int;
type tSegmentId = int;

// A log entry consisting of the sequence number and the value
type tLogEntry = (seqNum: tLogSeqNum, val: tLogValue);
type tSegmentKey = (logId: tLogId, segmentId: tSegmentId);

// Append
type tAppendRequest = (client: Producer, segmentKey: tSegmentKey, seqNum: tLogSeqNum, val: tLogValue);
enum tAppendResponseStatus { APPEND_OK, APPEND_SEG_CLOSED }
type tAppendResponse = (status: tAppendResponseStatus, segmentKey: tSegmentKey, seqNum: tLogSeqNum, logNode: LogNode);
event eAppendRequest : tAppendRequest;
event eAppendResponse : tAppendResponse;

// Read
type tReadRequest = (client: Consumer, segmentKey: tSegmentKey, offset: int);
enum tReadResponseStatus { READ_OK, READ_NO_MORE_ENTRIES, READ_NEW_SEGMENT }
type tReadResponse = (status: tReadResponseStatus, segmentKey: tSegmentKey, offset: int, seqNum: tLogSeqNum, val: tLogValue);
event eReadRequest : tReadRequest;
event eReadResponse : tReadResponse;

// Spec events
type tLogEntryStored = (logId: tLogId, seqNum: tLogSeqNum, logNode: LogNode);
event eLogEntryStored : tLogEntryStored;

enum tSegmentStatus { SEG_OPEN, SEG_CLOSED, SEG_NOT_INIT }

/* 
The LogNode is responsible for writing the log entries of the producers 
to the log and sending them to the consumers upon request.
*/
machine LogNode {
    // Individual segments holding the log entries
    var logEntries: map[tSegmentKey, seq[tLogEntry]];
    // Status for each segment
    var logStatus: map[tSegmentKey, tSegmentStatus];

    start state Init  {
        entry {
            goto WaitForRequest;
        }
        defer eShutDown;
    }

    state WaitForRequest {
        on eAppendRequest do (req: tAppendRequest) {
            // Initializes segment if needed
            if (!(req.segmentKey in logEntries)) {
                logEntries[req.segmentKey] = default(seq[tLogEntry]);
                logStatus[req.segmentKey] = SEG_OPEN;
            }
            
            // We do not allow writes to closed segments
            if (logStatus[req.segmentKey] == SEG_CLOSED) {
                UnreliableSend(req.client, eAppendResponse, (status = APPEND_SEG_CLOSED, segmentKey = req.segmentKey, seqNum = req.seqNum, logNode = this));
                return;
            }

            // Make sure the sequence number is new
            if (sizeof(logEntries[req.segmentKey]) > 0) {
                assert req.seqNum > logEntries[req.segmentKey][sizeof(logEntries[req.segmentKey])-1].seqNum;
            }

            // Append the log entry to the segment
            announce eLogEntryStored, (logId = req.segmentKey.logId, seqNum = req.seqNum, logNode = this);            
            logEntries[req.segmentKey] += (sizeof(logEntries[req.segmentKey]), (seqNum = req.seqNum, val = req.val));
            print format("Stored sequence number {0} on node {1} (segment: {2}, size: {3})", req.seqNum, this, req.segmentKey.segmentId, sizeof(logEntries[req.segmentKey]));

            UnreliableSend(req.client, eAppendResponse, (status = APPEND_OK, segmentKey = req.segmentKey, seqNum = req.seqNum, logNode = this));
        }

        on eReadRequest do (req: tReadRequest) {
            var logEntry: tLogEntry;
            var numSegmentEntries: int;

            // We don't even know about the segment yet. No entries were published thus.
            if (!(req.segmentKey in logEntries)) {
                UnreliableSend(req.client, eReadResponse, (status = READ_NO_MORE_ENTRIES, segmentKey = req.segmentKey, offset = req.offset, seqNum = 0, val = 0));
                return;
            }

            // Usually, it is fine to return all log entries but the last one of the segment to the consumer. The
            // reason not to return the last log entry is because we do not know if the log entry is successfully
            // replicated to all other log nodes holding the current segment. Only if that is the case, the log 
            // entry is actually committed and we can return it to the consumer.

            // The only exception are closed segments. When closing a segment the producer informs the log nodes 
            // via the coordinator how many log entries can be considered committed. This makes sure that the last
            // log entry is either truncated (if the producer does not consider it committed) or reported to the 
            // consumer (if the producer has retrieved acknowledgements from all other log nodes)
            numSegmentEntries = sizeof(logEntries[req.segmentKey]);
            if (logStatus[req.segmentKey] == SEG_CLOSED && req.offset == numSegmentEntries - 1) {
                logEntry = logEntries[req.segmentKey][req.offset];
                UnreliableSend(req.client, eReadResponse, (status = READ_OK, segmentKey = req.segmentKey, offset = req.offset, seqNum = logEntry.seqNum, val = logEntry.val));
                return;
            }

            // On an open segment, we do not report a log entry immediately to the consumers because we 
            // cannot be certain it was replicated already to all other log nodes.
            // This works since by contract the producers only write a single log message concurrently. When
            // the next one is written, we know the previous one will be replicated as needed already.
            // -1 <= 0
            if (numSegmentEntries - 1 <= req.offset) {
                if (logStatus[req.segmentKey] == SEG_CLOSED) {
                    print format("Sending SEG_CLOSED on offset {0} (segment: {1} (size: {2}), node {3})", req.offset, req.segmentKey.segmentId, sizeof(logEntries[req.segmentKey]), this);

                    UnreliableSend(req.client, eReadResponse, (status = READ_NEW_SEGMENT, segmentKey = req.segmentKey, offset = req.offset, seqNum = 0, val = 0));
                    return;
                }

                UnreliableSend(req.client, eReadResponse, (status = READ_NO_MORE_ENTRIES, segmentKey = req.segmentKey, offset = req.offset, seqNum = 0, val = 0));
                return;
            }
            logEntry = logEntries[req.segmentKey][req.offset];
            UnreliableSend(req.client, eReadResponse, (status = READ_OK, segmentKey = req.segmentKey, offset = req.offset, seqNum = logEntry.seqNum, val = logEntry.val));
        }

        on eEndSegment do (req: tEndSegment) {
            var segmentKey: tSegmentKey;
            segmentKey = (logId = req.logId, segmentId = req.segment);

            logStatus[segmentKey] = SEG_CLOSED;
            if (!(segmentKey in logEntries)) {
                logEntries[segmentKey] = default(seq[tLogEntry]);
            }
            // Cut off all uncommitted entries so we will not report those to the consumers.
            while (sizeof(logEntries[segmentKey]) > req.numEntries) {
                logEntries[segmentKey] -= (sizeof(logEntries[segmentKey]) - 1);
            }
            assert sizeof(logEntries[segmentKey]) == req.numEntries;
            print format("Declare SEG_CLOSED sequence number {0} on node {1}", req.segment, this);

            UnreliableSend(req.coordinator, eEndSegmentResponse, (node = this, logId = req.logId, segment = req.segment));
        }

        on eShutDown do {
            print format("Log node {0} is down", this);

            raise halt;
        }
    }
}
