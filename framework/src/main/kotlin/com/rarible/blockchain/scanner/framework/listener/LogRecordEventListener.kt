package com.rarible.blockchain.scanner.framework.listener

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

/**
 * Client side LogRecordEvent listener
 */
interface LogRecordEventListener {
    val id: String

    val groupId: String

    suspend fun onLogRecordEvents(events: List<LogRecordEvent>)
}