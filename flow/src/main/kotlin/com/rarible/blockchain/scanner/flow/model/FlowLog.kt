package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Log
import java.time.Instant

data class FlowLog(
    override val transactionHash: String,
    override val status: Log.Status,
    val txIndex: Int?,
    val eventIndex: Int?,
    val type: String?,
    val payload: String?,
    val timestamp: Instant,
    val blockHeight: Long,
    val errorMessage: String? = null
): Log
