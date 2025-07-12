type tMonitor_Progress = (numEntries: int, totalProducers: int);
event eMonitor_Progress: tMonitor_Progress;

/* 
Liveness: checks that all log entries of the last producer are eventually reported to a client.
*/
spec Progress observes eMonitor_Progress, eReadResponse {
    var reportedLogEntries: set[int];
    var numLogEntries: int;
    var totalProducers: int;

    start state Init {
        on eMonitor_Progress do (req: tMonitor_Progress) {
        	numLogEntries = req.numEntries;
            totalProducers = req.totalProducers;
            goto WaitForReplies;
        }
    }

    hot state WaitForReplies {
        on eReadResponse do (resp: tReadResponse) {
            if (resp.status != READ_OK) {
                return;
            }

            // We can only be sure that the last producer can write all its log entries
            if (resp.seqNum < numLogEntries * (totalProducers - 1)) {
                print format("Ignoring sequence number {0}", resp.seqNum);
                return;
            }

            reportedLogEntries += (resp.seqNum);
            print format("Reported sequence number {0} (total: {1}). Received log entries: {2}", resp.seqNum, numLogEntries - 1, reportedLogEntries);
            
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
