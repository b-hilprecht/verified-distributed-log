event eMonitor_SingleProducer;

type tProducerLogPair = (logId: tLogId, producer: Producer);
type tLogEntrySent = (producer: Producer, logId: tLogId, seqNum: tLogSeqNum);
event eLogEntrySent: tLogEntrySent;

type tProducerTakeOver = (logId: tLogId, producer: Producer);
event eProducerTakeOver : tProducerTakeOver;

/*
Safety: makes sure there is only a single producer able to write log entries.
*/
spec SingleActiveProducer observes eLogEntryCommitted, eLogEntrySent, eProducerTakeOver, eMonitor_SingleProducer
{
	var currentProducers: map[tLogId, Producer];
	var highestPossibleCommit: map[tProducerLogPair, tLogSeqNum];

	start state Init {
		on eMonitor_SingleProducer goto WaitForEvents;
	}

	state WaitForEvents {
		on eProducerTakeOver do (resp: tProducerTakeOver) {
			currentProducers[resp.logId] = resp.producer;
		}

		on eLogEntryCommitted do (resp: tLogEntryCommitted) {
			var prodLogPair: tProducerLogPair;
			prodLogPair = (logId = resp.logId, producer = resp.producer);

			// Currently the producer. All ok
			if (resp.logId in currentProducers && currentProducers[resp.logId] == resp.producer)
				return;

			// Could be that this is a commit from a previous try when the producer was still active
			assert prodLogPair in highestPossibleCommit, 
			format("Producer log pair {0} was not observed as producer", prodLogPair);

			assert highestPossibleCommit[prodLogPair] >= resp.seqNum,
			format("Seq number {0} by producer {1} is committed even though it is not currently the producer and was not tried while it was", resp.seqNum, resp.producer);
		}

		on eLogEntrySent do (resp: tLogEntrySent) {
			var prodLogPair: tProducerLogPair;
			prodLogPair = (logId = resp.logId, producer = resp.producer);

			// we can only consider this as the highestPossibleCommit if it
			// is currently the active producer
			if (currentProducers[resp.logId] != resp.producer)
				return;
			
			highestPossibleCommit[prodLogPair] = resp.seqNum;
		}
	}
}
