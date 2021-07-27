package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.model.Log

class LogEvent<L : Log>(
    val log: L,
    val collection: String
)