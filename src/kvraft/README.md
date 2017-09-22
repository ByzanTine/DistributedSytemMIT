# 1. Try to ensure that one op per log.
Not possible.
If a client rf.Start() and then timeout, we never know if the log will be commited.
