package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Log
import org.springframework.data.annotation.Id
import java.time.LocalDateTime

data class FlowLog(
    override val transactionHash: String,
    override val status: Log.Status,
    val txIndex: Int,
    val eventIndex: Int,
    val type: String,
    val payload: String,
    val timestamp: LocalDateTime,
    val blockHeight: Long
): Log {

    @get:Id
    val id: String
    get() = "${transactionHash}:$eventIndex"
}
