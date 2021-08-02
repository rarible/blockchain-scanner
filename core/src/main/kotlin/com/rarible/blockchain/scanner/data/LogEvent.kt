package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogEventDescriptor

class LogEvent<L : Log, D : LogEventDescriptor>(
    val log: L,
    val descriptor: D
)