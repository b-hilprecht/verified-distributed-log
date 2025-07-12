test tcMultiReplica [main=MultiReplicaWithFailure]:
  assert ReadCommitted, CommitDurability, Progress, SingleWriter in
  (union Log, {MultiReplicaWithFailure}, CreateProducers, FailureInjectorWithNotification, Timer);
