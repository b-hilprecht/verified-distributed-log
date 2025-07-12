event eMonitor_Progress: int;

/* 
Liveness: checks that all log entries are eventually reported to a client.
*/
spec Progress observes eMonitor_Progress, eReadResponse {
    var reportedLogEntries: set[int];
    var numLogEntries: int;

    start state Init {
        on eMonitor_Progress goto WaitForReplies with (n: int) {
        	numLogEntries = n;
        }
    }

    hot state WaitForReplies {
        on eReadResponse do (resp: tReadResponse) {
            if (resp.status != READ_OK) {
                return;
            }

            reportedLogEntries += (resp.seqNum);
            print format("Reported sequence number {0}. Received log entries: {1}", resp.seqNum, reportedLogEntries);
            
            // We only expect all log entries up the last one to be reported
            // to the consumers.
            if (sizeof(reportedLogEntries) == numLogEntries - 1) {
                goto AllLogEntriesConsumed;
            }
        }
    }

    cold state AllLogEntriesConsumed {
        ignore eReadResponse;
    }
}
