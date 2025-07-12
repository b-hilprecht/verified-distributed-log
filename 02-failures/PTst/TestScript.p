test tcMultiReplica [main=MultiReplicaWithFailure]:
  assert ReadCommitted, CommitDurability, Progress in
  (union Log, {MultiReplicaWithFailure}, FailureInjectorWithNotification, Timer);
