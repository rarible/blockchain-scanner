package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.model.LogEvent

class RichLogEvent<L : LogEvent>(
    val log: L,
    val collection: String
)