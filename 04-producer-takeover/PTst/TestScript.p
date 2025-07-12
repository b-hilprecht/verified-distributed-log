test tcMultiReplica [main=MultiReplicaWithFailure]:
  assert ReadCommitted, CommitDurability, Progress, SingleActiveProducer in
  (union Log, {MultiReplicaWithFailure}, CreateProducers, FailureInjectorWithNotification, Timer);
