/*
This injector triggers a new producer to continue to produce log entries
*/
event eDelayTakeover;

machine CreateProducers {
	var totalTakeovers: int;
	var numTakeovers: int;
	var numLogEntries: int;
	var logId: tLogId;
	var coordinator: Coordinator;
	var takeOverProducer: Producer;
	var timer: Timer;

	start state Init {
		entry (config: (totalProducers: int, coordinator: Coordinator, numLogEntries: int, logId: tLogId)) {
			totalTakeovers = config.totalProducers - 1;
			coordinator = config.coordinator;
			numLogEntries = config.numLogEntries;
			logId = config.logId;

			timer = CreateTimer(this);

			takeOverProducer = new Producer((coordinator = coordinator, currentEntry = numTakeovers * numLogEntries, numEntries = (numTakeovers + 1) * numLogEntries, announceDone = (totalTakeovers == numTakeovers)));
			send takeOverProducer, eProducerTakeOverLog, logId;
			goto WaitForTakeover;
		}
	}

	state WaitForTakeover {
		entry {
			send takeOverProducer, eTookOverLog, (client = this, logId = logId);
			StartTimer(timer);
		}

		on eTookOverLogResponse do (response: tTookOverLogResponse) {
			if (response.producer != takeOverProducer)
				return;
			if (response.logId != logId)
				return;
			goto TakeoverProducer;
		}

		on eTimeOut do {
			goto WaitForTakeover;
		}
	}

	state TakeoverProducer {
		entry {
			if(numTakeovers == totalTakeovers) {
				send timer, halt;
				raise halt; // done with all takeovers
			} else {
				if(choose(100) == 0) {
					numTakeovers = numTakeovers + 1;
					takeOverProducer = new Producer((coordinator = coordinator, currentEntry = numTakeovers * numLogEntries, numEntries = (numTakeovers + 1) * numLogEntries, announceDone = (totalTakeovers == numTakeovers)));
					send takeOverProducer, eProducerTakeOverLog, logId;
					goto WaitForTakeover;
				} else {
					send this, eDelayTakeover;
				}
			}
		}

		on eDelayTakeover goto TakeoverProducer;

		ignore eTimeOut, eTookOverLogResponse;
	}
}

fun IntializeCreateProducers(config: (totalProducers: int, coordinator: Coordinator, numLogEntries: int, logId: tLogId)) {
	new CreateProducers(config);
}

module CreateProducers = { CreateProducers };
