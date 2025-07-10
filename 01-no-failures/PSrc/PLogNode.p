type tLogValue = int;
type tLogSeqNum = int;
type tSegmentId = int;

// a log entry consisting of the sequence number and the value
type tLogEntry = (seqNum: tLogSeqNum, val: tLogValue);

// Append
type tAppendRequest = (client: Producer, seqNum: tLogSeqNum, val: tLogValue);
enum tAppendResponseStatus { APPEND_OK }
type tAppendResponse = (status: tAppendResponseStatus, seqNum: tLogSeqNum, logNodeId: int);
event eAppendRequest : tAppendRequest;
event eAppendResponse : tAppendResponse;

// Read
type tReadRequest = (client: Consumer, offset: int);
enum tReadResponseStatus { READ_OK, READ_NO_MORE_ENTRIES }
type tReadResponse = (status: tReadResponseStatus, seqNum: tLogSeqNum, val: tLogValue);
event eReadRequest : tReadRequest;
event eReadResponse : tReadResponse;

// Spec events
type tLogEntryStored = (seqNum: tLogSeqNum, logNodeId: int);
event eLogEntryStored : tLogEntryStored;

/* 
The LogNode is responsible for writing the log entries of the producers 
to the log and sending them to the consumers upon request.
*/
machine LogNode {
    var log: seq[tLogEntry];
    var logNodeId: int;

    start state Init  {
        entry (payload: int) {
            logNodeId = payload;
            goto WaitForRequest;
        }
    }

    state WaitForRequest {
        on eAppendRequest do (req : tAppendRequest) {
            announce eLogEntryStored, (seqNum = req.seqNum, logNodeId = logNodeId);

            log += (sizeof(log), (seqNum = req.seqNum, val = req.val));

            send req.client, eAppendResponse, (status = APPEND_OK, seqNum = req.seqNum, logNodeId = logNodeId);
        }

        on eReadRequest do (readReq : tReadRequest) {
            // We do not report a log entry immediately to the consumers because we cannot be certain it 
            // was replicated already to all other Segment nodes. We want to avoid coordination so we do not
            // want to communicate with other Segment nodes at this point. We thus choose a simple solution:
            // we only report all log entries up to the last one to the consumers.

            // This works since by contract the producers only write a single log message concurrently. When
            // the next one is written, we know the previous one will be replicated as needed already.
            if (sizeof(log) - 1 <= readReq.offset) {
                send readReq.client, eReadResponse, (status = READ_NO_MORE_ENTRIES, seqNum = 0, val = 0);
                return;
            }
            
            send readReq.client, eReadResponse, (status = READ_OK, seqNum = log[readReq.offset].seqNum, val = log[readReq.offset].val);
        }
    }
}
