// unreliable send operation that drops messages on the ether nondeterministically
fun UnreliableSend(target: machine, message: event, payload: any) {
    // nondeterministically drop messages
    if(choose(10) != 0) send target, message, payload;
}
