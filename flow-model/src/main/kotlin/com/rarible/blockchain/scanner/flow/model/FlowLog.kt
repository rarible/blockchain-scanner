package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Log
import java.time.Instant

data class FlowLog(
    override val transactionHash: String,
    override val status: Log.Status,
    val eventIndex: Int,
    val eventType: String,
    val timestamp: Instant,
    val blockHeight: Long,
    val blockHash: String
) : Log<FlowLog> {

    override fun withStatus(status: Log.Status): FlowLog {
        return this.copy(status = status)
    }
}
