package com.rarible.blockchain.scanner.framework.listener

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

/**
 * Client side subscriber for LogRecordEvent
 */
interface LogRecordEventSubscriber {
    suspend fun onLogRecordEvents(events: List<LogRecordEvent>)
}