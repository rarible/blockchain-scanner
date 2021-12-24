package com.rarible.blockchain.scanner.flow.model

import java.time.Instant

data class FlowLog(
    val transactionHash: String,
    val eventIndex: Int,
    val eventType: String,
    val timestamp: Instant,
    val blockHeight: Long,
    val blockHash: String
)
