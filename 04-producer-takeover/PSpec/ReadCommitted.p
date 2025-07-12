event eMonitor_ReadCommitted;

/*
Safety: Checks that only log replicas which are considered committed by the producer
are reported to the consumer.
*/
spec ReadCommitted observes eLogEntryCommitted, eReadResponse, eMonitor_ReadCommitted
{
	var committedEntries: set[tLogEntryCommit];

	start state Init {
		on eMonitor_ReadCommitted goto WaitForEvents;
	}

	state WaitForEvents {
		on eLogEntryCommitted do (resp: tLogEntryCommitted) {
			var logEntry: tLogEntryCommit;
			logEntry = (seqNum = resp.seqNum, logId = resp.logId);

			committedEntries += (logEntry);
		}

		on eReadResponse do (resp: tReadResponse) {
			var logEntry: tLogEntryCommit;

			if (resp.status != READ_OK) {
				return;
			}

			logEntry = (seqNum = resp.seqNum, logId = resp.segmentKey.logId);

			assert logEntry in committedEntries, 
			format("Log entry {0} is not considered committed but still replied to a consumer", logEntry);
		}
	}
}


