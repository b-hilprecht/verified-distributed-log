event eMonitor_SingleWriter;

type tProducerLogPair = (logId: tLogId, producer: Producer);
type tLogEntrySent = (producer: Producer, logId: tLogId, seqNum: tLogSeqNum);
event eLogEntrySent: tLogEntrySent;

type tProducerTakeOver = (logId: tLogId, producer: Producer);
event eProducerTakeOver : tProducerTakeOver;

/*
Safety: Checks that only log replicas which were replicated to a sufficient 
number of nodes are considered committed.
*/
spec SingleWriter observes eLogEntryCommitted, eLogEntrySent, eProducerTakeOver, eMonitor_SingleWriter
{
	var currentWriters: map[tLogId, Producer];
	var highestPossibleCommit: map[tProducerLogPair, tLogSeqNum];

	start state Init {
		on eMonitor_SingleWriter goto WaitForEvents;
	}

	state WaitForEvents {
		on eProducerTakeOver do (resp: tProducerTakeOver) {
			currentWriters[resp.logId] = resp.producer;
		}

		on eLogEntryCommitted do (resp: tLogEntryCommitted) {
			var prodLogPair: tProducerLogPair;
			prodLogPair = (logId = resp.logId, producer = resp.producer);

			// Currently the writer. All ok
			if (resp.logId in currentWriters && currentWriters[resp.logId] == resp.producer)
				return;

			// Could be that this is a commit from a previous try when the producer was still active
			assert prodLogPair in highestPossibleCommit, 
			format("Producer log pair {0} was not observed as writer", prodLogPair);

			assert highestPossibleCommit[prodLogPair] >= resp.seqNum,
			format("Seq number {0} by producer {1} is committed even though it is not currently the writer and was not tried while it was", resp.seqNum, resp.producer);
		}

		on eLogEntrySent do (resp: tLogEntrySent) {
			var prodLogPair: tProducerLogPair;
			prodLogPair = (logId = resp.logId, producer = resp.producer);

			if (currentWriters[resp.logId] != resp.producer)
				return;
			
			highestPossibleCommit[prodLogPair] = resp.seqNum;
		}
	}
}


