package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.Log

interface LogEventListener<L : Log> {

    val topics: Set<String>

    suspend fun onLogEvent(log: L)
}